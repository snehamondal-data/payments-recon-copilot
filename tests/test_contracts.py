import json

import pytest

from recon.contracts import ContractError, parse_message


def test_every_valid_generated_message_passes_its_contract(scenario):
    poison_values = {p.value for p in scenario.poison}
    for m in scenario.messages:
        if m.value not in poison_values:
            parse_message(m.topic, m.value)


def test_every_poison_message_is_rejected(scenario):
    for p in scenario.poison:
        with pytest.raises(ContractError):
            parse_message(p.topic, p.value)


def _doc(scenario, topic, **filters):
    for m in scenario.messages:
        if m.topic == topic:
            doc = json.loads(m.value)
            if all(doc.get(k) == v for k, v in filters.items()):
                return doc
    raise LookupError


def _reject(topic, doc) -> ContractError:
    with pytest.raises(ContractError) as exc:
        parse_message(topic, json.dumps(doc).encode())
    return exc.value


def test_rejects_unknown_fields(scenario):
    doc = _doc(scenario, "processor.events", event_type="charge") | {"surprise": 1}
    assert _reject("processor.events", doc).error_type == "contract_violation"


def test_types_are_strict_no_string_numbers(scenario):
    doc = _doc(scenario, "processor.events", event_type="charge")
    doc["amount_minor"] = str(doc["amount_minor"])
    _reject("processor.events", doc)


def test_rejects_float_money(scenario):
    doc = _doc(scenario, "processor.events", event_type="charge")
    doc["amount_minor"] = doc["amount_minor"] + 0.5
    _reject("processor.events", doc)


def test_processor_settlement_amount_must_follow_fx_rate(scenario):
    doc = _doc(scenario, "processor.events", event_type="charge", currency="EUR")
    doc["settlement_amount_minor"] += 1
    _reject("processor.events", doc)


def test_ledger_journal_must_balance(scenario):
    doc = _doc(scenario, "ledger.entries", entry_type="sale")
    doc["lines"][2]["credit_minor"] += 1
    assert "unbalanced" in _reject("ledger.entries", doc).detail


def test_ledger_posting_date_must_match_local_timestamp(scenario):
    doc = _doc(scenario, "ledger.entries", entry_type="sale")
    doc["posting_date"] = "2001-01-01"
    _reject("ledger.entries", doc)


def test_bank_rejects_weekend_value_date(scenario):
    doc = _doc(scenario, "bank.settlements")
    doc["value_date"] = "2026-10-10"  # a Saturday
    assert "weekend" in _reject("bank.settlements", doc).detail


def test_bank_batch_total_must_match_lines(scenario):
    doc = _doc(scenario, "bank.settlements")
    doc["batch_total_minor"] -= 1
    _reject("bank.settlements", doc)


def test_schema_version_is_pinned(scenario):
    doc = _doc(scenario, "ledger.entries") | {"schema_version": 2}
    _reject("ledger.entries", doc)


@pytest.mark.parametrize(("value", "error_type"), [
    (b"not json", "invalid_json"),
    (b"\xff\xfe\x00", "invalid_json"),
    (None, "empty_message"),
])
def test_garbage_bytes_raise_contract_error_not_crash(value, error_type):
    with pytest.raises(ContractError) as exc:
        parse_message("processor.events", value)
    assert exc.value.error_type == error_type


def test_unknown_topic():
    with pytest.raises(ContractError) as exc:
        parse_message("nope", b"{}")
    assert exc.value.error_type == "unknown_topic"
