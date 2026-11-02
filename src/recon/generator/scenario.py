"""Builds the synthetic Tallyline scenario: clean baseline traffic plus planted problems.

One seeded RNG and a simulated clock anchored at `as_of` drive everything, so the
same seed always produces byte-identical messages. Nothing reads the wall clock.
"""

from __future__ import annotations

import hashlib
import random
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

from recon.business_days import add_business_days, is_business_day
from recon.contracts import BankLine, BankSettlement, Contract, LedgerEntry, LedgerLine, ProcessorEvent
from recon.fx import FxRate, generate_fx_rates
from recon.money import processing_fee, to_minor
from recon.taxonomy import ExceptionType

LOCAL_TZ = ZoneInfo("America/New_York")  # Tallyline's ledger posts in local time
BASELINE_DAYS = 14
# The last baseline charge day is as_of - 4, so every baseline charge settles (T+1 or T+2) by as_of.
BASELINE_END_OFFSET_DAYS = 4
CURRENCIES = ("USD", "EUR", "GBP")
CURRENCY_WEIGHTS = (60, 25, 15)
REDELIVERIES = 5  # exact copies of already-sent messages, simulating at-least-once delivery


@dataclass(frozen=True)
class OutMessage:
    topic: str
    key: bytes | None
    value: bytes


@dataclass
class Charge:
    charge_id: str
    event: ProcessorEvent
    fee_usd: int
    event_ids: list[str] = field(default_factory=list)
    journal_ids: list[str] = field(default_factory=list)
    bank_line_ids: list[str] = field(default_factory=list)

    @property
    def gross_usd(self) -> int:
        return self.event.settlement_amount_minor

    @property
    def net_usd(self) -> int:
        return self.gross_usd - self.fee_usd

    @property
    def record_ids(self) -> list[str]:
        return [*self.event_ids, *self.journal_ids, *self.bank_line_ids]


@dataclass
class BankLineDraft:
    value_date: date
    line_id: str
    reference: str | None
    direction: str
    description: str
    net_amount_minor: int


@dataclass
class PlantedCase:
    case_id: str
    exception_type: ExceptionType
    expected_status: str | None  # None = decided in Slice 2 (date boundary)
    should_escalate: bool
    charge_ids: list[str]
    record_ids: list[str]
    description: str
    held_out: bool = False


@dataclass(frozen=True)
class PoisonMessage:
    topic: str
    kind: str
    key: bytes | None
    value: bytes


@dataclass
class Scenario:
    seed: int
    as_of: date
    messages: list[OutMessage]
    fx_rates: list[FxRate]
    processor_events: list[ProcessorEvent]
    bank_settlements: list[BankSettlement]
    ledger_entries: list[LedgerEntry]
    charges: dict[str, Charge]
    baseline_charge_ids: list[str]
    cases: list[PlantedCase]
    poison: list[PoisonMessage]
    redelivered: int

    def expected_counts(self) -> dict[str, int]:
        """Rows the loader should end up with: unique valid messages, poison and redeliveries excluded."""
        return {
            "processor_events": len(self.processor_events),
            "bank_settlement_batches": len(self.bank_settlements),
            "bank_settlement_lines": sum(len(b.lines) for b in self.bank_settlements),
            "ledger_journals": len(self.ledger_entries),
            "ledger_lines": sum(len(e.lines) for e in self.ledger_entries),
        }

    def content_hash(self) -> str:
        h = hashlib.sha256()
        for m in self.messages:
            h.update(m.topic.encode() + b"\0" + (m.key or b"") + b"\0" + m.value + b"\n")
        for r in self.fx_rates:
            h.update(f"{r.rate_date},{r.currency},{r.rate_to_usd}\n".encode())
        return h.hexdigest()


class ScenarioBuilder:
    def __init__(self, seed: int, as_of: date):
        self.seed = seed
        self.as_of = as_of
        self.rng = random.Random(seed)
        self.last_baseline_day = as_of - timedelta(days=BASELINE_END_OFFSET_DAYS)
        self.first_day = self.last_baseline_day - timedelta(days=BASELINE_DAYS - 1)
        self.fx_rates = generate_fx_rates(self.rng, self.first_day, as_of)
        self._fx = {(r.rate_date, r.currency): r.rate_to_usd for r in self.fx_rates}
        self.processor: list[ProcessorEvent] = []
        self.ledger: list[LedgerEntry] = []
        self.bank_lines: list[BankLineDraft] = []
        self.charges: dict[str, Charge] = {}
        self.baseline_charge_ids: list[str] = []
        self.cases: list[PlantedCase] = []
        self._used_ids: set[str] = set()

    # --- primitives ---------------------------------------------------------

    def new_id(self, prefix: str) -> str:
        while True:
            candidate = f"{prefix}_{self.rng.getrandbits(64):016x}"
            if candidate not in self._used_ids:
                self._used_ids.add(candidate)
                return candidate

    def fx_rate(self, day: date, currency: str) -> Decimal:
        return self._fx[(day, currency)]

    def amount(self, low: int = 500, high: int = 50_000) -> int:
        return self.rng.randint(low, high)

    def baseline_day(self, last_offset: int = 0) -> date:
        """A random day in the baseline window, optionally ending `last_offset` days early."""
        return self.first_day + timedelta(days=self.rng.randrange(BASELINE_DAYS - last_offset))

    def local_time(self, day: date, start_hour: int = 9, end_hour: int = 19) -> datetime:
        """A random local time on `day`, returned in UTC. Business hours never cross a UTC date."""
        t = time(self.rng.randrange(start_hour, end_hour), self.rng.randrange(60), self.rng.randrange(60))
        return datetime.combine(day, t, tzinfo=LOCAL_TZ).astimezone(UTC)

    def emit_processor(self, event_type: str, charge_id: str, amount: int, currency: str,
                       at: datetime) -> ProcessorEvent:
        rate = self.fx_rate(at.date(), currency)
        prefix = {"refund": "re", "fee": "fee", "dispute": "dp"}.get(event_type)
        event = ProcessorEvent(
            schema_version=1,
            event_id=self.new_id("evt"),
            event_type=event_type,
            object_id=charge_id if event_type == "charge" else self.new_id(prefix),
            charge_id=charge_id,
            amount_minor=amount,
            currency=currency,
            fx_rate=rate,
            settlement_amount_minor=to_minor(amount, rate),
            occurred_at=at,
        )
        self.processor.append(event)
        return event

    def emit_journal(self, charge_id: str, entry_type: str, posted_utc: datetime,
                     lines: list[tuple[str, int, int]]) -> LedgerEntry:
        posted_local = posted_utc.astimezone(LOCAL_TZ)
        entry = LedgerEntry(
            schema_version=1,
            event_id=self.new_id("evt"),
            journal_id=self.new_id("jrn"),
            charge_id=charge_id,
            entry_type=entry_type,
            currency="USD",
            posted_at=posted_local,
            posting_date=posted_local.date(),
            lines=tuple(LedgerLine(line_no=i, account=a, debit_minor=dr, credit_minor=cr)
                        for i, (a, dr, cr) in enumerate(lines, start=1)),
        )
        self.ledger.append(entry)
        return entry

    def emit_bank_line(self, value_date: date, reference: str | None, direction: str, description: str,
                       amount: int) -> BankLineDraft:
        assert is_business_day(value_date), f"bank line on a weekend: {value_date}"
        assert value_date <= self.as_of, f"bank line after as_of: {value_date}"
        draft = BankLineDraft(value_date, self.new_id("bln"), reference, direction, description, amount)
        self.bank_lines.append(draft)
        return draft

    def ledger_delay(self) -> timedelta:
        return timedelta(minutes=self.rng.randint(1, 10))

    # --- business events ----------------------------------------------------

    def charge(self, at: datetime, amount: int, currency: str = "USD", *, settle_lag: int | None = 1,
               ledger: bool = True, ledger_rate: Decimal | None = None, bank_net_delta: int = 0) -> Charge:
        """A charge, its fee, and (by default) its ledger sale journal and bank settlement line."""
        charge_id = self.new_id("ch")
        event = self.emit_processor("charge", charge_id, amount, currency, at)
        fee = processing_fee(event.settlement_amount_minor)
        fee_event = self.emit_processor("fee", charge_id, fee, "USD", at + timedelta(seconds=1))
        c = Charge(charge_id, event, fee, event_ids=[event.event_id, fee_event.event_id])
        if ledger:
            gross = to_minor(amount, ledger_rate) if ledger_rate is not None else c.gross_usd
            self.sale_journal(c, gross, at + self.ledger_delay())
        if settle_lag is not None:
            line = self.emit_bank_line(add_business_days(at.date(), settle_lag), charge_id, "credit",
                                       "SETTLEMENT", c.net_usd + bank_net_delta)
            c.bank_line_ids.append(line.line_id)
        self.charges[charge_id] = c
        return c

    def sale_journal(self, c: Charge, gross_usd: int, posted_utc: datetime) -> LedgerEntry:
        entry = self.emit_journal(c.charge_id, "sale", posted_utc, [
            ("processor_receivable", gross_usd - c.fee_usd, 0),
            ("processing_fees", c.fee_usd, 0),
            ("revenue", 0, gross_usd),
        ])
        c.journal_ids.append(entry.journal_id)
        return entry

    def refund(self, c: Charge, at: datetime, amount: int, *, ledger: bool = True,
               bank: bool = True) -> ProcessorEvent:
        event = self.emit_processor("refund", c.charge_id, amount, c.event.currency, at)
        c.event_ids.append(event.event_id)
        if bank:
            line = self.emit_bank_line(add_business_days(at.date(), 1), c.charge_id, "debit", "REFUND",
                                       event.settlement_amount_minor)
            c.bank_line_ids.append(line.line_id)
        if ledger:
            s = event.settlement_amount_minor
            entry = self.emit_journal(c.charge_id, "refund", at + self.ledger_delay(),
                                      [("refunds", s, 0), ("processor_receivable", 0, s)])
            c.journal_ids.append(entry.journal_id)
        return event

    def add_baseline(self, n: int) -> None:
        """Clean traffic that ties out across all three sources."""
        for _ in range(n):
            day = self.baseline_day()
            at = self.local_time(day)
            currency = self.rng.choices(CURRENCIES, weights=CURRENCY_WEIGHTS)[0]
            amount = self.amount()
            c = self.charge(at, amount, currency, settle_lag=self.rng.choice((1, 2)))
            self.baseline_charge_ids.append(c.charge_id)
            if self.rng.random() < 0.04:
                refund_at = self.local_time(day + timedelta(days=self.rng.randint(1, 2)))
                if add_business_days(refund_at.date(), 1) <= self.as_of:
                    full = self.rng.random() < 0.5
                    self.refund(c, refund_at, amount if full else self.rng.randint(100, amount - 1))

    # --- assembly -----------------------------------------------------------

    def _bank_batches(self) -> list[BankSettlement]:
        by_date: dict[date, list[BankLineDraft]] = {}
        for draft in self.bank_lines:
            by_date.setdefault(draft.value_date, []).append(draft)
        batches = []
        for value_date in sorted(by_date):
            lines = tuple(
                BankLine(line_no=i, line_id=d.line_id, reference=d.reference, direction=d.direction,
                         description=d.description, net_amount_minor=d.net_amount_minor)
                for i, d in enumerate(by_date[value_date], start=1)
            )
            batches.append(BankSettlement(
                schema_version=1,
                event_id=self.new_id("evt"),
                batch_id=self.new_id("bat"),
                value_date=value_date,
                currency="USD",
                lines=lines,
                batch_total_minor=sum(line.signed_amount_minor for line in lines),
            ))
        return batches

    def build(self) -> Scenario:
        from recon.generator.poison import build_poison

        batches = self._bank_batches()
        timeline: list[tuple[datetime, str, str, Contract]] = []
        timeline += [(e.occurred_at, "processor.events", e.charge_id, e) for e in self.processor]
        timeline += [(e.posted_at.astimezone(UTC), "ledger.entries", e.charge_id, e) for e in self.ledger]
        # Bank files arrive early morning UTC on the value date.
        timeline += [(datetime.combine(b.value_date, time(6), tzinfo=UTC), "bank.settlements", b.batch_id, b)
                     for b in batches]
        timeline.sort(key=lambda item: item[0])  # stable, so ties keep creation order
        messages = [OutMessage(topic, key.encode(), model.model_dump_json().encode())
                    for _, topic, key, model in timeline]

        # Redeliveries: the same bytes (same event_id) sent again a little later.
        for idx in sorted(self.rng.sample(range(len(messages)), REDELIVERIES), reverse=True):
            messages.insert(min(len(messages), idx + self.rng.randint(1, 40)), messages[idx])

        poison = build_poison(self, batches)
        for p in poison:
            messages.insert(self.rng.randrange(len(messages) + 1), OutMessage(p.topic, p.key, p.value))

        return Scenario(
            seed=self.seed,
            as_of=self.as_of,
            messages=messages,
            fx_rates=self.fx_rates,
            processor_events=list(self.processor),
            bank_settlements=batches,
            ledger_entries=list(self.ledger),
            charges=self.charges,
            baseline_charge_ids=self.baseline_charge_ids,
            cases=self.cases,
            poison=poison,
            redelivered=REDELIVERIES,
        )
