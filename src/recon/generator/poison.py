"""Deliberately invalid messages. Each one must end up in the DLQ, never in the warehouse."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from recon.contracts import BankSettlement
from recon.generator.scenario import PoisonMessage

if TYPE_CHECKING:
    from recon.generator.scenario import ScenarioBuilder


def _dump(doc: dict) -> bytes:
    return json.dumps(doc, separators=(",", ":")).encode()


def build_poison(b: ScenarioBuilder, batches: list[BankSettlement]) -> list[PoisonMessage]:
    charge = next(e for e in b.processor if e.event_type == "charge")
    sale = next(e for e in b.ledger if e.entry_type == "sale")
    batch = batches[0]

    def processor(**changes) -> dict:
        doc = charge.model_dump(mode="json") | {"event_id": b.new_id("evt")}
        return doc | changes

    def ledger() -> dict:
        return sale.model_dump(mode="json") | {"event_id": b.new_id("evt")}

    def bank() -> dict:
        return batch.model_dump(mode="json") | {"event_id": b.new_id("evt")}

    missing = processor()
    del missing["currency"]

    unbalanced = ledger()
    unbalanced["lines"][0]["debit_minor"] += 100

    naive = ledger()
    naive["posted_at"] = naive["posted_at"][:19]  # drop the UTC offset

    total_mismatch = bank()
    total_mismatch["batch_total_minor"] += 1

    key = charge.charge_id.encode()
    jkey = sale.charge_id.encode()
    bkey = batch.batch_id.encode()
    return [
        PoisonMessage("processor.events", "invalid_json", key, b'{"event_id": "evt_'),
        PoisonMessage("processor.events", "missing_field", key, _dump(missing)),
        PoisonMessage("processor.events", "negative_amount", key, _dump(processor(amount_minor=-charge.amount_minor))),
        PoisonMessage("processor.events", "unknown_currency", key, _dump(processor(currency="JPY"))),
        PoisonMessage("ledger.entries", "unbalanced_journal", jkey, _dump(unbalanced)),
        PoisonMessage("ledger.entries", "naive_timestamp", jkey, _dump(naive)),
        PoisonMessage("bank.settlements", "schema_version_2", bkey, _dump(bank() | {"schema_version": 2})),
        PoisonMessage("bank.settlements", "batch_total_mismatch", bkey, _dump(total_mismatch)),
    ]
