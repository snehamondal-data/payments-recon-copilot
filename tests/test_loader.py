from collections import Counter

import pytest
from confluent_kafka import KafkaError

from helpers import FakeDLQ, as_raw_messages, load_all
from recon.fx import read_fx_csv, write_fx_csv
from recon.loader.consume import run
from recon.loader.schema import SOURCE_TABLES, ensure_schema
from recon.loader.sink import Sink, load_fx_rates
from recon.warehouse import DuckDBClient


def table_counts(wh):
    counts = {t: wh.execute(f"SELECT count(*) FROM raw.{t}")[0][0] for t in SOURCE_TABLES}
    counts["dlq_log"] = wh.execute("SELECT count(*) FROM raw.dlq_log")[0][0]
    return counts


@pytest.fixture(scope="module")
def loaded(scenario):
    """One full load shared by the read-only tests below."""
    wh = DuckDBClient()
    ensure_schema(wh)
    dlq = FakeDLQ()
    raws = as_raw_messages(scenario)
    stats = load_all(Sink(wh, dlq), raws)
    yield wh, dlq, raws, stats
    wh.close()


def test_full_load_matches_expected_counts(scenario, loaded):
    wh, dlq, _, stats = loaded
    assert table_counts(wh) == scenario.expected_counts() | {"dlq_log": len(scenario.poison)}
    assert sum(s.rejected for s in stats) == len(scenario.poison)
    assert len(dlq.sent) == len(scenario.poison)


def test_redeliveries_are_skipped(scenario, loaded):
    stats = loaded[3]
    header_rows = sum(s.inserted[t] for s in stats
                      for t in ("processor_events", "bank_settlement_batches", "ledger_journals"))
    assert sum(s.valid for s in stats) - header_rows == scenario.redelivered


def test_dlq_records_carry_source_coordinates_and_reason(scenario, loaded):
    _, dlq, raws, _ = loaded
    by_coord = {(r.topic, r.partition, r.offset): r for r in raws}
    for record in dlq.sent:
        raw = by_coord[(record.source_topic, record.source_partition, record.source_offset)]
        assert record.raw_value_b64 is not None
        assert record.error_type in {"invalid_json", "contract_violation"}
        assert raw.value in {p.value for p in scenario.poison}
    assert Counter(r.source_topic for r in dlq.sent) == Counter(p.topic for p in scenario.poison)


def test_loaded_values_round_trip(scenario, loaded):
    e = next(e for e in scenario.processor_events if e.currency == "EUR")
    row = loaded[0].execute(
        "SELECT amount_minor, fx_rate, settlement_amount_minor, epoch(occurred_at) "
        "FROM raw.processor_events WHERE event_id = ?", (e.event_id,))[0]
    assert row == (e.amount_minor, e.fx_rate, e.settlement_amount_minor, e.occurred_at.timestamp())


def test_ledger_balances_in_the_warehouse(loaded):
    assert loaded[0].execute(
        "SELECT count(*) FROM (SELECT event_id FROM raw.ledger_lines GROUP BY 1 "
        "HAVING sum(debit_minor) <> sum(credit_minor))")[0][0] == 0


def test_replay_inserts_zero_rows_and_dead_letters_nothing_new(scenario, warehouse):
    raws = as_raw_messages(scenario)
    dlq = FakeDLQ()
    sink = Sink(warehouse, dlq)
    load_all(sink, raws)
    before = table_counts(warehouse)
    replay = load_all(sink, raws, batch_size=137)  # different batch boundaries on purpose
    assert table_counts(warehouse) == before
    assert sum(s.rows_inserted for s in replay) == 0
    assert sum(s.dead_lettered for s in replay) == 0
    assert len(dlq.sent) == len(scenario.poison)


def test_crash_before_offset_commit_redelivers_without_duplicates(scenario, warehouse):
    raws = as_raw_messages(scenario)
    sink = Sink(warehouse, FakeDLQ())
    sink.process(raws[:300])  # loaded, but pretend the process died before committing offsets
    load_all(sink, raws)      # Kafka redelivers from the last committed offset (here: the start)
    assert table_counts(warehouse) == scenario.expected_counts() | {"dlq_log": len(scenario.poison)}


def test_fx_rates_load_idempotently(scenario, warehouse, tmp_path):
    path = tmp_path / "fx.csv"
    write_fx_csv(path, scenario.fx_rates)
    load_fx_rates(warehouse, read_fx_csv(path))
    load_fx_rates(warehouse, read_fx_csv(path))
    assert warehouse.execute("SELECT count(*) FROM ref.fx_rates")[0][0] == len(scenario.fx_rates)


def test_a_failed_insert_rolls_back_the_whole_batch(scenario, warehouse, monkeypatch):
    sink = Sink(warehouse, FakeDLQ())
    real = warehouse.execute
    inserts = {"n": 0}

    def fail_on_second_insert(sql, params=()):
        if sql.startswith("INSERT"):
            inserts["n"] += 1
            if inserts["n"] == 2:
                raise RuntimeError("disk full")
        return real(sql, params)

    monkeypatch.setattr(warehouse, "execute", fail_on_second_insert)
    with pytest.raises(RuntimeError):
        sink.process(as_raw_messages(scenario)[:200])
    assert all(n == 0 for n in table_counts(warehouse).values())


# --- the consume loop (fake consumer, no Kafka) -------------------------------

class FakeError:
    def __init__(self, code):
        self._code = code

    def code(self):
        return self._code


class FakeMsg:
    def __init__(self, raw=None, eof=None):
        self.raw, self.eof = raw, eof

    def error(self):
        return FakeError(KafkaError._PARTITION_EOF) if self.eof else None

    def topic(self):
        return self.eof[0] if self.eof else self.raw.topic

    def partition(self):
        return self.eof[1] if self.eof else self.raw.partition

    def offset(self):
        return self.raw.offset

    def key(self):
        return self.raw.key

    def value(self):
        return self.raw.value


class FakeConsumer:
    def __init__(self, raws, events, batch=250):
        self.parts = sorted({(r.topic, r.partition) for r in raws})
        self.queue = [FakeMsg(r) for r in raws]
        self.events, self.batch = events, batch
        self.commits = []

    def consume(self, num_messages, timeout):
        out, self.queue = self.queue[:self.batch], self.queue[self.batch:]
        if not out:
            return [FakeMsg(eof=p) for p in self.parts]
        return out

    def commit(self, offsets, asynchronous):
        assert asynchronous is False
        self.events.append("offsets.commit")
        self.commits.append({(tp.topic, tp.partition): tp.offset for tp in offsets})

    def assignment(self):
        class TP:
            def __init__(self, t, p):
                self.topic, self.partition = t, p
        return [TP(t, p) for t, p in self.parts]


class RecordingWarehouse:
    dialect = "duckdb"

    def __init__(self, wh, events):
        self._wh, self.events = wh, events

    def execute(self, *a):
        return self._wh.execute(*a)

    def executemany(self, *a):
        return self._wh.executemany(*a)

    def transaction(self):
        outer = self

        class Tx:
            def __enter__(self):
                self._cm = outer._wh.transaction()
                self._cm.__enter__()

            def __exit__(self, *exc):
                result = self._cm.__exit__(*exc)
                if exc[0] is None:
                    outer.events.append("db.commit")
                return result
        return Tx()

    def close(self):
        self._wh.close()


def test_offsets_are_committed_only_after_dlq_flush_and_db_commit(scenario, warehouse):
    events: list[str] = []
    raws = as_raw_messages(scenario)
    consumer = FakeConsumer(raws, events)
    stats = run(consumer, Sink(RecordingWarehouse(warehouse, events), FakeDLQ(events)), log=lambda _: None)
    assert stats.received == len(raws)
    for i, event in enumerate(events):
        if event == "offsets.commit":
            assert events[i - 1] == "db.commit"
    assert events.index("dlq.flush") < events.index("db.commit")
    # The final commit points one past the last offset of every partition.
    last = {}
    for r in raws:
        last[(r.topic, r.partition)] = r.offset + 1
    merged = {}
    for c in consumer.commits:
        merged |= c
    assert merged == last


def test_sink_failure_means_no_offset_commit(scenario, warehouse, monkeypatch):
    events: list[str] = []
    consumer = FakeConsumer(as_raw_messages(scenario), events)
    sink = Sink(warehouse, FakeDLQ())
    monkeypatch.setattr(sink, "process", lambda batch: (_ for _ in ()).throw(RuntimeError("db down")))
    with pytest.raises(RuntimeError):
        run(consumer, sink, log=lambda _: None)
    assert consumer.commits == []


def test_sink_strips_topic_prefix_but_logs_physical_topic(scenario, warehouse):
    raws = [r.__class__(f"it.{r.topic}", r.partition, r.offset, r.key, r.value) for r in as_raw_messages(scenario)]
    load_all(Sink(warehouse, FakeDLQ(), topic_prefix="it."), raws)
    assert table_counts(warehouse) == scenario.expected_counts() | {"dlq_log": len(scenario.poison)}
    assert {r[0] for r in warehouse.execute("SELECT DISTINCT _topic FROM raw.ledger_journals")} == {"it.ledger.entries"}
