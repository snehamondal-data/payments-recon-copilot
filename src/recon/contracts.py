"""Message contracts: one pydantic model per source topic, plus the dead-letter envelope.

Rules shared by every contract:
- unknown fields are rejected, and types are strict (no "123" -> 123 coercion);
- money is integer minor units (cents), never floats;
- timestamps must carry a timezone;
- schema_version is pinned, so an upstream schema change lands in the DLQ instead of the warehouse.
"""

from __future__ import annotations

import base64
import json
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Annotated, Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, ValidationError, model_validator

from recon.money import to_minor

Currency = Literal["USD", "EUR", "GBP"]
PositiveMinor = Annotated[int, Field(gt=0)]
NonNegativeMinor = Annotated[int, Field(ge=0)]
EventId = Annotated[str, Field(pattern=r"^evt_[0-9a-f]{16}$")]
ChargeId = Annotated[str, Field(pattern=r"^ch_[0-9a-f]{16}$")]

OBJECT_PREFIX = {"charge": "ch_", "refund": "re_", "fee": "fee_", "dispute": "dp_"}


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class Contract(_Strict):
    schema_version: Literal[1]
    # Unique per message. For the processor it is the webhook delivery ID, which is the loader's dedupe key.
    event_id: EventId


class ProcessorEvent(Contract):
    event_type: Literal["charge", "refund", "fee", "dispute"]
    object_id: Annotated[str, Field(pattern=r"^(ch|re|fee|dp)_[0-9a-f]{16}$")]
    charge_id: ChargeId
    amount_minor: PositiveMinor
    currency: Currency
    fx_rate: Annotated[Decimal, Field(gt=0, max_digits=12, decimal_places=6)]
    settlement_amount_minor: PositiveMinor
    occurred_at: AwareDatetime

    @model_validator(mode="after")
    def _consistent(self) -> ProcessorEvent:
        if not self.object_id.startswith(OBJECT_PREFIX[self.event_type]):
            raise ValueError(f"object_id {self.object_id!r} does not match event_type {self.event_type!r}")
        if self.event_type == "charge" and self.object_id != self.charge_id:
            raise ValueError("a charge's object_id must equal its charge_id")
        if self.event_type == "fee" and self.currency != "USD":
            raise ValueError("fees are charged in USD")
        if self.currency == "USD" and self.fx_rate != 1:
            raise ValueError("USD events must have fx_rate 1")
        if self.settlement_amount_minor != to_minor(self.amount_minor, self.fx_rate):
            raise ValueError("settlement_amount_minor must equal amount_minor * fx_rate, rounded half up")
        if self.occurred_at.utcoffset() != timedelta(0):
            raise ValueError("processor occurred_at must be UTC")
        return self


class BankLine(_Strict):
    line_no: Annotated[int, Field(ge=1)]
    line_id: Annotated[str, Field(pattern=r"^bln_[0-9a-f]{16}$")]
    # Usually the processor charge_id. Null when the bank received no remittance reference.
    reference: Annotated[str, Field(min_length=1, max_length=64)] | None
    direction: Literal["credit", "debit"]
    description: Literal["SETTLEMENT", "REFUND", "CHARGEBACK", "DEPOSIT"]
    net_amount_minor: PositiveMinor

    @property
    def signed_amount_minor(self) -> int:
        return self.net_amount_minor if self.direction == "credit" else -self.net_amount_minor


class BankSettlement(Contract):
    batch_id: Annotated[str, Field(pattern=r"^bat_[0-9a-f]{16}$")]
    value_date: date
    currency: Literal["USD"]
    lines: Annotated[tuple[BankLine, ...], Field(min_length=1)]
    batch_total_minor: int

    @model_validator(mode="after")
    def _consistent(self) -> BankSettlement:
        if self.value_date.weekday() >= 5:
            raise ValueError("the bank does not settle on weekends")
        if [line.line_no for line in self.lines] != list(range(1, len(self.lines) + 1)):
            raise ValueError("line_no must run 1..n in order")
        if self.batch_total_minor != sum(line.signed_amount_minor for line in self.lines):
            raise ValueError("batch_total_minor does not equal the signed sum of the lines")
        return self


class LedgerLine(_Strict):
    line_no: Annotated[int, Field(ge=1)]
    account: Literal["processor_receivable", "processing_fees", "revenue", "refunds", "chargebacks", "dispute_fees"]
    debit_minor: NonNegativeMinor
    credit_minor: NonNegativeMinor

    @model_validator(mode="after")
    def _one_side(self) -> LedgerLine:
        if (self.debit_minor > 0) == (self.credit_minor > 0):
            raise ValueError("a ledger line is either a debit or a credit")
        return self


class LedgerEntry(Contract):
    journal_id: Annotated[str, Field(pattern=r"^jrn_[0-9a-f]{16}$")]
    charge_id: ChargeId
    entry_type: Literal["sale", "refund", "fee", "chargeback"]
    currency: Literal["USD"]
    # Local time with its offset; posting_date is the local calendar date.
    posted_at: AwareDatetime
    posting_date: date
    lines: Annotated[tuple[LedgerLine, ...], Field(min_length=2)]

    @model_validator(mode="after")
    def _consistent(self) -> LedgerEntry:
        if [line.line_no for line in self.lines] != list(range(1, len(self.lines) + 1)):
            raise ValueError("line_no must run 1..n in order")
        if sum(line.debit_minor for line in self.lines) != sum(line.credit_minor for line in self.lines):
            raise ValueError("journal is unbalanced: debits != credits")
        if self.posting_date != self.posted_at.date():
            raise ValueError("posting_date must be the local date of posted_at")
        return self


CONTRACTS: dict[str, type[Contract]] = {
    "processor.events": ProcessorEvent,
    "bank.settlements": BankSettlement,
    "ledger.entries": LedgerEntry,
}


class ContractError(Exception):
    def __init__(self, error_type: str, detail: str):
        super().__init__(f"{error_type}: {detail}")
        self.error_type = error_type
        self.detail = detail


def parse_message(topic: str, value: bytes | None) -> Contract:
    """Validate a raw message against its topic's contract. Raises ContractError, never anything else."""
    model = CONTRACTS.get(topic)
    if model is None:
        raise ContractError("unknown_topic", topic)
    if value is None:
        raise ContractError("empty_message", "message has no value")
    try:
        return model.model_validate_json(value)
    except ValidationError as err:
        errors = err.errors(include_url=False, include_input=False)
        error_type = "invalid_json" if any(e["type"] == "json_invalid" for e in errors) else "contract_violation"
        detail = json.dumps([{"loc": list(e["loc"]), "msg": e["msg"]} for e in errors], default=str)
        raise ContractError(error_type, detail) from None
    except UnicodeDecodeError as err:
        raise ContractError("invalid_json", str(err)) from None


class DLQRecord(BaseModel):
    """What the loader publishes to the dead-letter topic for each rejected message."""

    source_topic: str
    source_partition: int
    source_offset: int
    key: str | None
    error_type: str
    error: str
    raw_value_b64: str | None
    failed_at: datetime

    @classmethod
    def build(cls, topic: str, partition: int, offset: int, key: bytes | None, value: bytes | None,
              err: ContractError) -> DLQRecord:
        return cls(
            source_topic=topic,
            source_partition=partition,
            source_offset=offset,
            key=key.decode("utf-8", "replace") if key is not None else None,
            error_type=err.error_type,
            error=err.detail,
            raw_value_b64=base64.b64encode(value).decode() if value is not None else None,
            failed_at=datetime.now(timezone.utc),
        )
