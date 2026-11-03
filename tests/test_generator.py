from collections import Counter, defaultdict
from datetime import UTC, timedelta
from decimal import ROUND_HALF_UP, Decimal

import pytest

from recon.business_days import is_business_day, settlement_deadline
from recon.generator import build_scenario
from recon.generator.planted import CASES_PER_TYPE, LEGACY_LEDGER_RATE_QUANTUM
from recon.money import processing_fee, to_minor
from recon.taxonomy import ExceptionType as T


@pytest.fixture(scope="module")
def idx(scenario):
    """Lookups by charge_id / reference over the generated records."""
    events, journals, lines = defaultdict(list), defaultdict(list), defaultdict(list)
    for e in scenario.processor_events:
        events[e.charge_id].append(e)
    for j in scenario.ledger_entries:
        journals[j.charge_id].append(j)
    for b in scenario.bank_settlements:
        for line in b.lines:
            lines[line.reference].append((b.value_date, line))
    return events, journals, lines


def cases_of(scenario, exception_type):
    return [c for c in scenario.cases if c.exception_type == exception_type]


def receivable(journal) -> int:
    return next(line.debit_minor for line in journal.lines if line.account == "processor_receivable")


# --- determinism and coverage -------------------------------------------------

def test_same_seed_gives_identical_output(scenario):
    again = build_scenario()
    assert again.content_hash() == scenario.content_hash()
    assert [m.value for m in again.messages] == [m.value for m in scenario.messages]


def test_different_seed_gives_different_output(scenario):
    assert build_scenario(seed=7).content_hash() != scenario.content_hash()


def test_every_section_3_type_is_planted(scenario):
    counts = Counter(c.exception_type for c in scenario.cases)
    assert set(counts) == set(T)
    assert all(n == CASES_PER_TYPE for n in counts.values())
    assert len(scenario.cases) >= 20


def test_held_out_cases_leave_at_least_two_dev_cases_per_type(scenario):
    held_out = [c for c in scenario.cases if c.held_out]
    assert len(held_out) == 6
    for t in T:
        assert sum(not c.held_out for c in cases_of(scenario, t)) >= 2


def test_case_record_ids_exist_in_the_data(scenario):
    known = {e.event_id for e in scenario.processor_events}
    known |= {j.journal_id for j in scenario.ledger_entries}
    known |= {line.line_id for b in scenario.bank_settlements for line in b.lines}
    for c in scenario.cases:
        assert c.record_ids and set(c.record_ids) <= known, c.case_id


def test_500_baseline_transactions(scenario):
    assert len(scenario.baseline_charge_ids) == 500


# --- calendar -----------------------------------------------------------------

def test_bank_never_settles_on_weekends_or_after_as_of(scenario):
    for b in scenario.bank_settlements:
        assert is_business_day(b.value_date)
        assert b.value_date <= scenario.as_of


def test_baseline_ties_out_within_t_plus_2_business_days(scenario, idx):
    events, journals, lines = idx
    for charge_id in scenario.baseline_charge_ids:
        c = scenario.charges[charge_id]
        value_date, line = next((vd, ln) for vd, ln in lines[charge_id] if ln.description == "SETTLEMENT")
        charge_date = c.event.occurred_at.date()
        assert charge_date < value_date <= settlement_deadline(charge_date)
        assert line.net_amount_minor == c.gross_usd - processing_fee(c.gross_usd)
        sale = next(j for j in journals[charge_id] if j.entry_type == "sale")
        assert receivable(sale) == line.net_amount_minor
        assert sale.posting_date == charge_date  # baseline never crosses the date boundary


# --- FX -----------------------------------------------------------------------

def test_fx_table_covers_every_day_and_currency(scenario):
    days = {r.rate_date for r in scenario.fx_rates}
    first, last = min(days), max(days)
    assert last == scenario.as_of
    assert len(days) == (last - first).days + 1
    assert Counter(r.currency for r in scenario.fx_rates) == {"USD": len(days), "EUR": len(days), "GBP": len(days)}


def test_processor_fx_rate_comes_from_reference_table(scenario):
    ref = {(r.rate_date, r.currency): r.rate_to_usd for r in scenario.fx_rates}
    for e in scenario.processor_events:
        assert e.fx_rate == ref[(e.occurred_at.date(), e.currency)]
        assert e.settlement_amount_minor == to_minor(e.amount_minor, e.fx_rate)


def test_currency_rounding_is_derivable_from_reference_table(scenario, idx):
    _, journals, _ = idx
    ref = {(r.rate_date, r.currency): r.rate_to_usd for r in scenario.fx_rates}
    for case in cases_of(scenario, T.CURRENCY_ROUNDING):
        c = scenario.charges[case.charge_ids[0]]
        rate = ref[(c.event.occurred_at.date(), c.event.currency)]
        legacy_gross = to_minor(c.event.amount_minor, rate.quantize(LEGACY_LEDGER_RATE_QUANTUM, ROUND_HALF_UP))
        sale = journals[c.charge_id][0]
        ledger_gross = next(ln.credit_minor for ln in sale.lines if ln.account == "revenue")
        assert ledger_gross == legacy_gross
        assert abs(ledger_gross - c.gross_usd) in (1, 2)


# --- planted problems ---------------------------------------------------------

def test_late_settlements_are_unsettled_and_still_in_window(scenario, idx):
    _, journals, lines = idx
    deadlines = []
    for case in cases_of(scenario, T.LATE_SETTLEMENT):
        assert case.expected_status == "pending_in_window"
        c = scenario.charges[case.charge_ids[0]]
        assert not lines[c.charge_id]
        assert journals[c.charge_id]
        deadlines.append(settlement_deadline(c.event.occurred_at.date()))
    assert all(d >= scenario.as_of for d in deadlines)
    assert scenario.as_of in deadlines, "the edge-of-window case must have its deadline on the as-of date"


def test_missing_settlements_are_past_their_window(scenario, idx):
    _, _, lines = idx
    for case in cases_of(scenario, T.MISSING_SETTLEMENT):
        c = scenario.charges[case.charge_ids[0]]
        assert not lines[c.charge_id]
        assert settlement_deadline(c.event.occurred_at.date()) < scenario.as_of


def test_duplicate_webhook_is_same_object_new_delivery_id(scenario, idx):
    events, journals, lines = idx
    for case in cases_of(scenario, T.DUPLICATE_WEBHOOK):
        charges = [e for e in events[case.charge_ids[0]] if e.event_type == "charge"]
        assert len(charges) == 2
        assert charges[0].event_id != charges[1].event_id
        assert charges[0].model_dump(exclude={"event_id"}) == charges[1].model_dump(exclude={"event_id"})
        assert len([j for j in journals[case.charge_ids[0]] if j.entry_type == "sale"]) == 2
        assert len(lines[case.charge_ids[0]]) == 1


def test_fee_mismatch_bank_net_is_short(scenario, idx):
    _, _, lines = idx
    for case in cases_of(scenario, T.FEE_MISMATCH):
        c = scenario.charges[case.charge_ids[0]]
        short = c.net_usd - lines[c.charge_id][0][1].net_amount_minor
        assert 25 <= short <= 75


def test_partial_refund_missing_from_ledger(scenario, idx):
    events, journals, lines = idx
    for case in cases_of(scenario, T.PARTIAL_REFUND_NOT_IN_LEDGER):
        charge_id = case.charge_ids[0]
        refund = next(e for e in events[charge_id] if e.event_type == "refund")
        assert refund.amount_minor < scenario.charges[charge_id].event.amount_minor
        assert any(ln.description == "REFUND" for _, ln in lines[charge_id])
        assert not any(j.entry_type == "refund" for j in journals[charge_id])


def test_missing_ledger_entry(scenario, idx):
    _, journals, lines = idx
    for case in cases_of(scenario, T.MISSING_LEDGER_ENTRY):
        assert lines[case.charge_ids[0]] and not journals[case.charge_ids[0]]


def test_orphan_deposit_matches_no_charge(scenario):
    for case in cases_of(scenario, T.ORPHAN_BANK_DEPOSIT):
        line = next(ln for b in scenario.bank_settlements for ln in b.lines if ln.line_id == case.record_ids[0])
        assert line.reference not in scenario.charges
        assert line.direction == "credit"


def test_chargeback_debits_after_settlement_with_no_ledger_reversal(scenario, idx):
    events, journals, lines = idx
    for case in cases_of(scenario, T.CHARGEBACK):
        charge_id = case.charge_ids[0]
        dispute = next(e for e in events[charge_id] if e.event_type == "dispute")
        settled = next(vd for vd, ln in lines[charge_id] if ln.description == "SETTLEMENT")
        debit_date, debit = next((vd, ln) for vd, ln in lines[charge_id] if ln.description == "CHARGEBACK")
        assert settled < dispute.occurred_at.date() < debit_date
        assert debit.direction == "debit" and debit.net_amount_minor == dispute.amount_minor + 1500
        assert [j.entry_type for j in journals[charge_id]] == ["sale"]


def test_date_boundary_puts_sources_on_different_days(scenario, idx):
    _, journals, lines = idx
    for case in cases_of(scenario, T.DATE_BOUNDARY):
        assert case.expected_status is None  # decided in Slice 2
        c = scenario.charges[case.charge_ids[0]]
        sale = journals[c.charge_id][0]
        assert c.event.occurred_at.date() == sale.posting_date + timedelta(days=1)
        assert receivable(sale) == lines[c.charge_id][0][1].net_amount_minor


def test_ambiguous_cases_must_escalate_and_really_are_ambiguous(scenario, idx):
    events, journals, lines = idx
    variants = set()
    for case in cases_of(scenario, T.AMBIGUOUS):
        assert case.should_escalate
        if len(case.charge_ids) == 2:  # one unreferenced deposit, two candidates
            variants.add("A")
            c1, c2 = (scenario.charges[cid] for cid in case.charge_ids)
            assert c1.net_usd == c2.net_usd
            assert not lines[c1.charge_id] and not lines[c2.charge_id]
            assert any(ln.net_amount_minor == c1.net_usd for _, ln in lines[None])
        else:  # shortfall == unledgered refund == plausible fee error
            variants.add("B")
            c = scenario.charges[case.charge_ids[0]]
            refund = next(e for e in events[c.charge_id] if e.event_type == "refund")
            settled = lines[c.charge_id][0][1].net_amount_minor
            assert c.net_usd - settled == refund.settlement_amount_minor
            assert not any(j.entry_type == "refund" for j in journals[c.charge_id])
    assert variants == {"A", "B"}
    assert all(c.should_escalate is False for c in scenario.cases if c.exception_type != T.AMBIGUOUS)


def test_poison_and_redeliveries_are_in_the_stream(scenario):
    assert len(scenario.poison) == 8
    assert len({p.kind for p in scenario.poison}) == 8
    values = Counter(m.value for m in scenario.messages)
    assert sum(n - 1 for n in values.values() if n > 1) == scenario.redelivered


def test_processor_timestamps_are_utc(scenario):
    assert all(e.occurred_at.tzinfo == UTC for e in scenario.processor_events)


def test_every_case_names_the_records_that_prove_it(scenario):
    for c in scenario.cases:
        assert c.required_record_ids, c.case_id
        assert set(c.required_record_ids) <= set(c.record_ids), c.case_id


def test_required_evidence_has_the_right_kind_of_record(scenario, idx):
    events, journals, lines = idx
    kinds = {e.event_id: e.event_type for e in scenario.processor_events}
    for c in cases_of(scenario, T.FEE_MISMATCH):
        assert kinds[c.required_record_ids[0]] == "fee" and c.required_record_ids[1].startswith("bln_")
    for c in cases_of(scenario, T.CHARGEBACK):
        assert kinds[c.required_record_ids[0]] == "dispute"
    for c in cases_of(scenario, T.DUPLICATE_WEBHOOK):
        assert [kinds[r] for r in c.required_record_ids] == ["charge", "charge"]
