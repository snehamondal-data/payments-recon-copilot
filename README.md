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
| 2 | Reconciliation: dbt staging, matching, `recon_matches`, `recon_exceptions`, `recon_pending` | done |
| 3 | Copilot: typed read-only tools, validated tool-use loop, review CLI, audit log | done |
| 4 to 9 | Evals, Snowflake, Airflow, docs, PDFs, Kubernetes | not started |

## Quickstart (Mac on Apple Silicon)

Needs Docker and Python 3.12 or newer. All images and wheels are native arm64.

```bash
make venv              # create .venv and install
make up                # start Kafka in KRaft mode and create the topics
make generate          # build the scenario (seed 42) and publish it
make load              # load into data/warehouse.duckdb, stop at the end of each partition
make replay            # re-read every topic from the start: inserts zero rows
make recon             # dbt build: models plus dbt tests (as-of date from AS_OF)
make test              # unit tests (no Docker, no API calls)
make test-integration  # end to end against the local broker
make test-live         # one real Claude call (needs ANTHROPIC_API_KEY in .env; costs a few cents)
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

The rule lives in [src/recon/business_days.py](src/recon/business_days.py). The dbt models mirror it
with a business-day calendar ([int_business_calendar.sql](dbt/models/intermediate/int_business_calendar.sql)),
and a test checks that the SQL and Python give the same deadline for every day of the calendar.

## What Slice 1 builds

```
generator ──► processor.events, bank.settlements, ledger.entries ──► loader ──► DuckDB raw.* tables
                                                                       │              │
                                            invalid messages ──► dlq topic        dbt (Slice 2)
                                                                 + raw.dlq_log        │
                                                                    recon_matches, recon_exceptions, recon_pending
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

## Reconciliation (Slice 2)

The dbt project in [dbt/](dbt/) runs on DuckDB now and is written to run unchanged on Snowflake in Slice 5:

- Dialect differences (UTC date of a timestamp, ISO day of week) sit behind `adapter.dispatch` macros
  in [dbt/macros/portability.sql](dbt/macros/portability.sql), with a DuckDB and a Snowflake version each.
- Everything else uses dbt's cross-database macros (`dateadd`, `concat`, `listagg`, `type_string`).
- A test scans every model and dbt test for DuckDB-only syntax.

```
staging        stg_processor_events, stg_bank_settlement_lines, stg_ledger_journals, stg_fx_rates
intermediate   int_business_calendar, int_processor_charges (+ T+2 deadline), int_bank_by_reference,
               int_ledger_by_charge, int_unreferenced_line_candidates, int_unreferenced_lines,
               int_charge_recon (reason_code + window_status), int_bank_line_exceptions
marts          recon_matches, recon_exceptions, recon_pending
```

**Matching.** Charges are matched on `charge_id` across all three sources. For the bank, the match is
on the line's `reference`.

A bank settlement with no reference is matched on amount instead. A candidate charge has no referenced
settlement of its own, the same expected net amount, and the line's value date inside its window.

- Exactly one candidate: the line settles that charge.
- No candidates: the line is an orphan.
- More than one candidate: the line is `ambiguous`.

**Window status.** Each charge is `matched`, `pending_in_window` or `exception`.

- An unsettled charge becomes an exception (`missing_settlement`) only after its T+2 business-day
  deadline has passed.
- Until then it waits in `recon_pending`.

**Reason codes**, checked in this priority order:

| Reason code | Meaning |
|---|---|
| `ambiguous` | a deposit fits several charges, a shortfall equals both an unbooked refund and a plausible fee error, or a ledger/bank difference fits no rule: escalate |
| `duplicate_webhook` | the processor delivered the same charge more than once |
| `missing_ledger_entry` | no sale journal |
| `missing_settlement` | unsettled after the window closed |
| `chargeback` | disputed and debited, with no ledger reversal |
| `partial_refund_not_in_ledger` | processor refunds exceed ledger refunds |
| `fee_mismatch` | bank net differs from gross minus the processor fee |
| `currency_rounding` | ledger differs from bank by 1 or 2 cents on a non-USD charge |
| `date_mismatch` | amounts tie, but the ledger dates it on a different day (timing only) |
| `orphan_bank_deposit` | a bank line that names no known charge, or has no candidate |

Each row of `recon_exceptions` has these fields:

- `exception_id`: `charge:<id>` or `bank_line:<id>`.
- `reason_code`.
- `amount_minor`: the amount at stake, in USD cents (0 for `date_mismatch`).
- `age_business_days`: business days since the charge date or value date, up to as-of.
- `window_status`.
- `related_charge_ids`: the candidate charges, for an ambiguous deposit.

**Checks:**

- dbt tests cover unique grain on every model, matched totals reconciling (row by row and in total),
  late settlements never being exceptions, deadlines being T+2 business days, and every charge being
  classified exactly once.
- A pytest ([tests/test_reconciliation.py](tests/test_reconciliation.py)) loads a fresh warehouse and
  runs `dbt build`. It then scores `recon_exceptions` against the ground-truth file: every planted case
  must appear with the right reason code, the late settlements must not appear, and there must be no
  unexplained exceptions.

## The copilot (Slice 3)

The copilot investigates one exception at a time and **proposes** a resolution. A human approves or
rejects every proposal, and approving only ever writes a resolution record. Source data is never
changed.

```bash
python -m recon.copilot queue                         # exceptions + proposal/review state
python -m recon.copilot diagnose charge:ch_...        # or --all
python -m recon.copilot show charge:ch_...            # diagnosis, evidence records, tool trace
python -m recon.copilot approve charge:ch_... --reason "..."
python -m recon.copilot reject  charge:ch_... --reason "..."
python -m recon.copilot resolve charge:ch_... --action post_fee_adjustment --reason "..."
```

**The agent never writes SQL.** It can call six typed tools: `get_exception`, `get_processor_events`,
`get_bank_settlement`, `get_ledger_entries`, `get_related_refunds` and `get_fx_rate`. Each tool:

- validates its arguments with pydantic (ID patterns, real dates, a currency enum) before any query runs;
- runs one fixed, parameterised `SELECT`;
- returns records that each carry a `record_id`.

The connection is DuckDB `read_only` with external access disabled and the configuration locked. A
stray `ATTACH` of the ground-truth file, a file read or a write all fail, and tests check each one.

**Structured output.** The model finishes by calling `submit_diagnosis`. The submission is validated
with pydantic:

- `root_cause`: one of the 10 reconciliation reason codes.
- `evidence`: record IDs that must have been returned by a tool in that same investigation.
- `proposed_action`, `confidence`, `needs_human_escalation`, `rationale`.

An invalid submission gets one retry. After that comes the **deterministic fallback**: the
reconciliation reason code, `escalate_to_human`, confidence 0, flagged `is_fallback`.

**Safety contract** ([contract.py](src/recon/copilot/contract.py)). `escalate_to_human` is always
allowed; for `ambiguous` it is the only option.

| root_cause | allowed actions besides escalation |
|---|---|
| `ambiguous` | none |
| `missing_settlement` | `request_settlement_trace` |
| `duplicate_webhook` | `reverse_duplicate_journal` |
| `missing_ledger_entry` | `post_missing_sale_journal` |
| `partial_refund_not_in_ledger` | `post_refund_journal` |
| `chargeback` | `post_chargeback_journal` |
| `fee_mismatch` | `raise_fee_claim_with_processor`, `post_fee_adjustment` |
| `currency_rounding` | `post_fx_rounding_adjustment` |
| `orphan_bank_deposit` | `move_to_suspense` |
| `date_mismatch` | `mark_timing_difference` (no money moves) |

**Guard rails:**

- Each investigation has a budget of 8 tool calls and a 90-second timeout.
- API errors, refusals and truncated responses end in the fallback.
- A diagnosis with confidence below 0.6 is downgraded to an escalation, and the override is logged.
- Text inside records (a bank reference, say) is treated as data, never as instructions. A test
  plants an instruction in a bank reference, and even a model that obeys it is stopped by validation.

**Blind mode.** With `COPILOT_HIDE_REASON_CODE=true`, `get_exception` doesn't return the
reconciliation reason code, so the agent must work out the cause itself. `show` still displays the
code to the reviewer.

**Audit log.** `data/copilot.duckdb` holds an append-only `audit_log` and a `resolutions` table. The
log records every tool call with its arguments, every raw model output, any validation failure or
policy override, the final diagnosis, and every review decision, each with a timestamp. Unit tests
use a scripted fake model, so they make no API calls and cost nothing. The model and the key come
from `.env` (`RECON_MODEL`, default `claude-sonnet-5-5`; `ANTHROPIC_API_KEY`).

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
