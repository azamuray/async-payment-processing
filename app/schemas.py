import json
from datetime import datetime
from decimal import Decimal
from typing import Annotated
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, JsonValue, field_validator

from app.models import Currency, Status


class PaymentCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    amount: Annotated[Decimal, Field(gt=0, max_digits=18, decimal_places=2)]
    currency: Currency
    description: Annotated[str, Field(min_length=1, max_length=1000, pattern=r"^[^\x00]+$")]
    metadata: dict[str, JsonValue] = Field(default_factory=dict)
    webhook_url: HttpUrl

    @field_validator("metadata")
    @classmethod
    def validate_metadata(cls, value: dict[str, JsonValue]) -> dict[str, JsonValue]:
        encoded = json.dumps(value, ensure_ascii=False, allow_nan=False)
        # JSONB cannot store NUL. Ignore escaped backslashes before checking for the NUL escape.
        if r"\u0000" in encoded.replace(r"\\", ""):
            raise ValueError("Metadata must not contain NUL characters")
        encoded.encode("utf-8")
        return value

    @field_validator("webhook_url")
    @classmethod
    def validate_webhook_url(cls, value: HttpUrl) -> HttpUrl:
        if value.username is not None or value.password is not None or value.fragment is not None:
            raise ValueError("Webhook URL must not contain credentials or a fragment")
        return value


class PaymentAccepted(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    payment_id: UUID = Field(validation_alias="id")
    status: Status
    created_at: datetime


class PaymentDetails(PaymentAccepted):
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, JsonValue] = Field(validation_alias="extra_data")
    idempotency_key: str
    webhook_url: HttpUrl
    processed_at: datetime | None


class PaymentEvent(BaseModel):
    event_id: UUID
    payment_id: UUID


class WebhookEvent(BaseModel):
    event_id: UUID
    payment_id: UUID
    status: Status
    amount: Decimal
    currency: Currency
    processed_at: datetime
