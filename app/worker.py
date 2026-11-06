import asyncio
import logging
import os
import random
import signal
from datetime import UTC, datetime, timedelta

import httpx
from faststream import AckPolicy
from faststream.rabbit import Channel, QueueType, RabbitBroker, RabbitExchange, RabbitMessage, RabbitQueue
from pamqp.commands import Basic
from sqlalchemy import select

from app.db import engine, sessions
from app.models import Outbox, Payment, Status
from app.schemas import PaymentEvent, WebhookEvent

logger = logging.getLogger(__name__)
MAX_ATTEMPTS = 3
WEBHOOK_TIMEOUT = 5
POLL_INTERVAL = 0.5

events = RabbitExchange("payments", durable=True)
dead_letters = RabbitExchange("payments.dlx", durable=True)
dlq = RabbitQueue("payments.dlq", durable=True)
new_payments = RabbitQueue(
    "payments.new",
    queue_type=QueueType.QUORUM,
    durable=True,
    arguments={
        "x-dead-letter-exchange": dead_letters.name,
        "x-dead-letter-routing-key": dlq.name,
        "x-dead-letter-strategy": "at-least-once",
        "x-overflow": "reject-publish",
        "x-single-active-consumer": True,
    },
)
broker = RabbitBroker(
    os.environ["AMQP_URL"],
    default_channel=Channel(prefetch_count=1, publisher_confirms=True, on_return_raises=True),
    graceful_timeout=35,
)


class UnknownPaymentEvent(Exception):
    pass


async def publish_one() -> bool:
    async with sessions.begin() as db:
        row = await db.scalar(
            select(Outbox)
            .where(Outbox.published_at.is_(None))
            .order_by(Outbox.created_at, Outbox.id)
            .limit(1)
            .with_for_update(skip_locked=True)
        )
        if row is None:
            return False

        event = PaymentEvent(event_id=row.id, payment_id=row.payment_id)
        # Cancellation also bounds time spent waiting for a disconnected channel.
        async with asyncio.timeout(5):
            confirmation = await broker.publish(
                event.model_dump(mode="json"),
                exchange=events,
                routing_key=new_payments.name,
                mandatory=True,
                persist=True,
                message_id=str(event.event_id),
            )
        if not isinstance(confirmation, Basic.Ack):
            raise RuntimeError(f"RabbitMQ did not confirm event {event.event_id}")
        row.published_at = datetime.now(UTC)
    return True


async def relay_outbox() -> None:
    while True:
        try:
            if await publish_one():
                continue
        except Exception:
            logger.exception("Outbox publication failed; event remains unpublished")
        await asyncio.sleep(POLL_INTERVAL)


async def process_payment(event: PaymentEvent) -> None:
    async with sessions.begin() as db:
        payment = await db.scalar(
            select(Payment)
            .join(Outbox)
            .where(Payment.id == event.payment_id, Outbox.id == event.event_id)
            .with_for_update(of=Payment)
        )
        if payment is None:
            raise UnknownPaymentEvent(f"Unknown event {event.event_id} for {event.payment_id}")
        if payment.status == Status.PENDING:
            await asyncio.sleep(random.uniform(2, 5))
            payment.status = Status.SUCCEEDED if random.random() < 0.9 else Status.FAILED
            payment.processed_at = datetime.now(UTC)


async def send_webhook(
    event: PaymentEvent, client: httpx.AsyncClient
) -> tuple[bool, bool]:
    """Return whether the webhook was delivered and whether attempts are exhausted."""
    async with sessions.begin() as db:
        # Serialize delivery with redeliveries, including after a consumer failover.
        payment = await db.get(Payment, event.payment_id, with_for_update=True)
        if payment.webhook_delivered_at is not None:
            return True, False
        if payment.webhook_failures >= MAX_ATTEMPTS:
            return False, True
        if payment.webhook_retry_at is not None:
            delay = (payment.webhook_retry_at - datetime.now(UTC)).total_seconds()
            await asyncio.sleep(max(0, delay))

        payload = WebhookEvent(
            event_id=event.event_id,
            payment_id=payment.id,
            status=payment.status,
            amount=payment.amount,
            currency=payment.currency,
            processed_at=payment.processed_at,
        )
        try:
            async with asyncio.timeout(WEBHOOK_TIMEOUT):
                async with client.stream(
                    "POST",
                    payment.webhook_url,
                    json=payload.model_dump(mode="json"),
                    headers={"Idempotency-Key": str(event.event_id)},
                ) as response:
                    response.raise_for_status()
        except (httpx.HTTPError, TimeoutError) as exc:
            payment.webhook_failures += 1
            exhausted = payment.webhook_failures == MAX_ATTEMPTS
            payment.webhook_retry_at = (
                None
                if exhausted
                else datetime.now(UTC) + timedelta(seconds=2 ** (payment.webhook_failures - 1))
            )
            logger.warning(
                "Webhook failed for %s (%d/%d, %s)",
                event.payment_id, payment.webhook_failures, MAX_ATTEMPTS, type(exc).__name__,
            )
            return False, exhausted

        payment.webhook_delivered_at = datetime.now(UTC)
        payment.webhook_retry_at = None
        return True, False


@broker.subscriber(new_payments, events, ack_policy=AckPolicy.MANUAL, no_reply=True)
async def handle_payment(message: RabbitMessage) -> None:
    try:
        event = PaymentEvent.model_validate(await message.decode())
    except (ValueError, TypeError):
        logger.warning("Invalid payment event; routing to DLQ")
        await message.reject()
        return

    async with httpx.AsyncClient(follow_redirects=False, trust_env=False) as client:
        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                await process_payment(event)
                delivered, exhausted = await send_webhook(event, client)
            except UnknownPaymentEvent:
                logger.warning("Unknown payment event %s; routing to DLQ", event.event_id)
                break
            except Exception:
                logger.exception("Processing failed for %s, attempt %d", event.payment_id, attempt)
                if attempt < MAX_ATTEMPTS:
                    await asyncio.sleep(2 ** (attempt - 1))
                continue

            if delivered:
                await message.ack()
                return
            if exhausted:
                break

    logger.error("Routing event %s to DLQ", event.event_id)
    await message.reject()


async def main() -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(signum, stop.set)
    try:
        async with broker:
            exchange = await broker.declare_exchange(dead_letters)
            queue = await broker.declare_queue(dlq)
            await queue.bind(exchange, routing_key=dlq.name)
            await broker.start()
            relay = asyncio.create_task(relay_outbox())
            try:
                await stop.wait()
            finally:
                relay.cancel()
                await asyncio.gather(relay, return_exceptions=True)
    finally:
        await engine.dispose()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    # httpx INFO contains full webhook URLs, including any tokens in query strings.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    asyncio.run(main())
