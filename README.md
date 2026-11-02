# payments-recon-copilot

> **Everything here is synthetic.** Tallyline is a fictional payments startup, and every transaction,
> settlement and ledger entry is generated from a fixed random seed. No real customer, bank, processor
> or money is involved.

A reconciliation pipeline for three sources that never quite agree (the payment processor, the bank and
the internal ledger), with an AI copilot that will investigate the exceptions and propose fixes for a
human to approve. The full plan is in [docs/PROJECT_SPEC.md](docs/PROJECT_SPEC.md).

Kafka, and later Snowflake and Airflow, are used at **project scale**: a single local broker, not a
production deployment.

## Status

| Slice | What | State |
|---|---|---|
| 1 | Data in: Kafka (KRaft), generator, contracts, DLQ, idempotent DuckDB loader | done |
| 2 to 9 | Reconciliation, copilot, evals, Snowflake, Airflow, docs, PDFs, Kubernetes | not started |

## Quickstart (Mac on Apple Silicon)

Needs Docker and Python 3.12 or newer. All images and wheels are native arm64.

```bash
make venv              # create .venv and install
make up                # start Kafka in KRaft mode and create the topics
make generate          # build the scenario (seed 42) and publish it
make load              # load into data/warehouse.duckdb, stop at the end of each partition
make replay            # re-read every topic from the start: inserts zero rows
make test              # unit tests (no Docker needed)
make test-integration  # end to end against the local broker
make down              # stop Kafka and delete its data
```

Settings come from environment variables; see [.env.example](.env.example).

## Settlement window: T+2 business days, inclusive

- The window counts **business days** (Monday to Friday) after the charge's **UTC date**. There is no
  holiday calendar yet.
- It is **inclusive**: a charge on Tuesday may settle as late as Thursday and still be on time.
  Examples: Thursday to Monday, Friday to Tuesday, Saturday or Sunday to Tuesday.
- The bank never settles on a weekend. The bank contract rejects a weekend `value_date`.
- Until its deadline has passed, an unsettled charge is `pending_in_window`, **not** an exception. The
  generator plants a case whose deadline is the as-of date itself, to test the inclusive edge.

The rule lives in [src/recon/business_days.py](src/recon/business_days.py).

## What Slice 1 builds

```
generator ──► processor.events, bank.settlements, ledger.entries ──► loader ──► DuckDB raw.* tables
                                                                       │
                                            invalid messages ──► dlq topic + raw.dlq_log
```

**Topics.** Three source topics with 3 partitions each, plus `dlq`. Topic retention is unlimited
(`retention.ms=-1`), so a replay from the beginning always sees the full history.

**Contracts** ([src/recon/contracts.py](src/recon/contracts.py)) are pydantic models with strict types:

- Unknown fields are rejected.
- Money is integer cents.
- Timestamps must carry a timezone.
- `schema_version` is pinned.

Each topic also has its own consistency checks:

- **Processor:** the settlement amount follows the FX rate.
- **Bank:** the batch total equals the signed sum of its lines.
- **Ledger:** debits equal credits.

**Loader** ([src/recon/loader/](src/recon/loader/)). Replay is safe because:

- Offsets are committed by hand, and only after the load succeeds.
- Every insert is `ON CONFLICT DO NOTHING` on `event_id` (or `(event_id, line_no)` for line tables), in
  one transaction per batch.
- A message that fails its contract goes to the `dlq` topic with its source coordinates and the reason.
  Nothing crashes.
- `raw.dlq_log` records each dead-lettered message, so a replay doesn't dead-letter it a second time.

**Duplicates: delivery vs business.**

- A Kafka redelivery has the *same* `event_id`, and the loader drops it.
- A duplicate *webhook* from the processor has a *new* `event_id` for the same charge. The loader keeps
  it, and reconciliation (Slice 2) must catch it.

**Reference FX.** `ref.fx_rates` has a rate to USD for every currency on every day of the scenario. The
processor converts at the rate for the event's UTC date. The planted rounding case is a ledger that
rounds that same rate to 4 decimal places.

## The scenario

The generator ([src/recon/generator/](src/recon/generator/)) uses one RNG seeded with 42 and a simulated
clock anchored at **as-of 2026-10-15**. The same seed always gives byte-identical output.

- **Baseline:** 500 clean transactions over 14 days, in USD, EUR and GBP, with a 2.9% + 30 cent fee.
- **Planted problems:** 33 cases, 3 of each type in spec section 3. 6 cases are held out for honest
  evals.
- **Messages:** 8 poison messages for the DLQ and 5 redeliveries.

The answers (planted cases, expected DLQ contents) go to **`data/ground_truth.duckdb`**, a separate file
from the warehouse. The loader and the agent tools never open it, and a test checks that the warehouse
contains no ground-truth tables.

All generated files live in `data/`, which is git-ignored and can be rebuilt with `make generate`.
