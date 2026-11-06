import os
import secrets
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, FastAPI, Header, HTTPException, Security
from fastapi.security import APIKeyHeader
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import engine, get_session
from app.models import Outbox, Payment
from app.schemas import PaymentAccepted, PaymentCreate, PaymentDetails

api_key = os.environ["API_KEY"].encode("utf-8")
if not api_key:
    raise RuntimeError("API_KEY must not be empty")


async def authenticate(
    supplied_key: Annotated[str | None, Security(APIKeyHeader(name="X-API-Key", auto_error=False))],
) -> None:
    if supplied_key is None or not secrets.compare_digest(supplied_key.encode("utf-8"), api_key):
        raise HTTPException(401, "Invalid or missing API key")


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    try:
        yield
    finally:
        await engine.dispose()


app = FastAPI(
    title="Async Payment Processing",
    lifespan=lifespan,
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)
router = APIRouter(prefix="/api/v1/payments", dependencies=[Depends(authenticate)])
Session = Annotated[AsyncSession, Depends(get_session)]
IdempotencyKey = Annotated[
    str, Header(alias="Idempotency-Key", min_length=1, max_length=255, pattern=r"^[!-~]+$")
]


@router.post("", status_code=202, response_model=PaymentAccepted)
async def create_payment(body: PaymentCreate, idempotency_key: IdempotencyKey, db: Session) -> Payment:
    async with db.begin():
        # PostgreSQL serializes concurrent inserts that use the same unique key.
        statement = (
            insert(Payment)
            .values(
                amount=body.amount,
                currency=body.currency,
                description=body.description,
                extra_data=body.metadata,
                idempotency_key=idempotency_key,
                webhook_url=str(body.webhook_url),
            )
            .on_conflict_do_nothing(index_elements=[Payment.idempotency_key])
            .returning(Payment)
        )
        payment = await db.scalar(statement)
        if payment is not None:
            db.add(Outbox(payment_id=payment.id))
            return payment

        existing = await db.execute(
            select(Payment, Payment.extra_data == body.metadata).where(
                Payment.idempotency_key == idempotency_key
            )
        )
        payment, same_metadata = existing.one()
        if (
            payment.amount != body.amount
            or payment.currency != body.currency
            or payment.description != body.description
            or payment.webhook_url != str(body.webhook_url)
            or not same_metadata
        ):
            raise HTTPException(409, "Idempotency-Key is already used with a different payload")
        return payment


@router.get("/{payment_id}", response_model=PaymentDetails)
async def get_payment(payment_id: UUID, db: Session) -> Payment:
    payment = await db.get(Payment, payment_id)
    if payment is None:
        raise HTTPException(404, "Payment not found")
    return payment


app.include_router(router)
