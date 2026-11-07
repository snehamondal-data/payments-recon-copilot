# Runbook

> **Project-scale and synthetic.** Kafka, Snowflake and Airflow run locally or on a trial account, and
> all data is generated. The procedures below are written as they would be for the customer, using
> this repo's real commands.

Five incidents, each with what you see, what to check, and what to do. Commands run from the repo
root unless noted.

Two rules hold in every incident:

- **Nothing is ever fixed by editing raw data or the marts.** The source of truth is the Kafka topics.
  Fix the cause, then reload or rebuild, which is safe because every load is idempotent and every daily
  task can be re-run for its date.
- **Never bulk-approve proposals to clear a backlog.** The copilot only proposes; a person decides
  every change.

Quick reference:

| Want to know | Command |
|---|---|
| The queue right now | `.venv/bin/python -m recon.copilot queue` (add `WAREHOUSE_ENGINE=snowflake` for Snowflake) |
| What each daily run saw | `make airflow-report FROM=2026-10-15 TO=2026-10-20` |
| A daily run's summary | the `summary` task log in the Airflow UI (the URL `astro dev start` prints) |
| Consumer lag | `docker exec recon-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group recon-airflow-loader` |
| Re-run one day | `cd airflow && astro dev run tasks clear recon_daily -s 2026-10-16 -e 2026-10-16 -y` |

For one-off queries on DuckDB:

```bash
.venv/bin/python -c "import duckdb; print(duckdb.connect('data/warehouse.duckdb', read_only=True).sql('''<SQL>'''))"
```

On Snowflake, use a Snowsight worksheet with the loader or agent role.

---

## 1. Exception spike

**You see:** the `summary` log or `make airflow-report` shows the backlog jumping, for example 30 to 80
in a day.

**Check:**

1. **Which reason code moved?** The summary lists the backlog by reason code. A spike in one code
   points to one cause:

   | Spiking code | Most likely cause |
   |---|---|
   | `missing_settlement` | the bank feed is late or stopped (go to [3. Kafka lag](#3-kafka-lag)), or windows closed over a weekend as designed |
   | `missing_ledger_entry` | the ledger feed stopped, or a ledger schema change is dead-lettering journals ([2](#2-upstream-schema-change)) |
   | `date_mismatch` | a timezone change in the ledger (local time vs UTC) |
   | `fee_mismatch` | the processor changed its fee schedule |
   | `orphan_bank_deposit`, `ambiguous` | bank references are missing (a bank format change) |
   | `duplicate_webhook` | the processor is retrying webhooks |

2. **Is it real, or late data?** Compare the newest data per source with the as-of date:
   ```sql
   SELECT 'bank' AS source, max(value_date) FROM raw.bank_settlement_batches
   UNION ALL SELECT 'ledger', max(posting_date) FROM raw.ledger_journals
   UNION ALL SELECT 'processor', CAST(max(occurred_at) AS DATE) FROM raw.processor_events;
   ```
   One source far behind the others means the "exceptions" are mostly missing data.

3. **Did the DLQ grow?** `SELECT topic, error_type, count(*) FROM raw.dlq_log GROUP BY 1, 2;`

**Do:**

- **Late or dead-lettered data:** fix the feed (see 2 or 3). Then re-run the affected dates, which is
  safe: `astro dev run tasks clear recon_daily -s <first day> -e <last day> -y`. The exceptions clear
  once the data arrives.
- **Real upstream change (fee schedule, timezone):** tell finance. Agree whether the reconciliation
  rule should change. Any rule change goes into dbt with a test and a planted eval case first.
- **Real problems at volume:** triage by amount at stake, `ORDER BY amount_minor DESC` on
  `recon_exceptions`. Diagnose the largest first, and escalate anything ambiguous.

---

## 2. Upstream schema change

**You see:** the DLQ topic and `raw.dlq_log` grow with `contract_violation` errors, usually followed a
day later by an exception spike (1) for that source. Nothing crashes: the loader dead-letters bad
messages and carries on.

**Check:**

```sql
SELECT topic, error_type, left(error, 200) AS error, count(*)
FROM raw.dlq_log GROUP BY 1, 2, 3 ORDER BY 4 DESC;
```

The error names the field: an unknown new field (contracts reject extras), a missing field, a changed
type, or a new `schema_version`. Each `raw_value` is the original message, so you can see exactly what
was sent.

**Do:**

1. **Talk to the source's owner** before changing anything. Is this a planned change? Will old-format
   messages still arrive?
2. **Change the contract** in [src/recon/contracts.py](../src/recon/contracts.py). Accept the new
   version alongside the old one; don't loosen validation. Add a unit test using a real dead-lettered
   `raw_value` as the fixture.
3. **If the change adds a column the warehouse needs,** add it to
   [loader/schema.py](../src/recon/loader/schema.py). On an existing warehouse also run
   `ALTER TABLE ... ADD COLUMN` as the loader, because `CREATE TABLE IF NOT EXISTS` won't add it.
   Update the dbt staging model and its tests.
4. **Replay from the beginning** with a fresh consumer group: `make replay` (DuckDB), or
   `WAREHOUSE_ENGINE=snowflake .venv/bin/python -m recon.loader --from-beginning`. Messages that were
   dead-lettered now load. Everything already loaded is skipped by the idempotent insert, which is
   measured at 0 duplicate rows on a full replay.
5. **Re-run the affected dates** of `recon_daily`, then `make parity` if both engines are in use.

**Don't** delete DLQ rows. They are the record of what arrived and when.

---

## 3. Kafka lag

**You see:** the daily summary shows few or no new rows loaded, the newest bank or ledger date falls
behind (query in 1), and `missing_settlement` creeps up.

**Check:**

```bash
docker exec recon-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group recon-airflow-loader
```

`LAG` per partition is how many messages are waiting. Then find out why they aren't being consumed:

| Finding | Cause |
|---|---|
| The `load_new_messages` task failed or never ran | Airflow is down, `recon_daily` is paused, or the task is stuck in the `recon_snowflake_writes` pool behind another task |
| The task fails with a Snowflake error | see [5](#5-snowflake-trial-expired-or-out-of-credits) |
| The task fails partway | by design, offsets are only committed after a successful load, so lag stays until a run succeeds; read the task log |
| `CURRENT-OFFSET` is not moving and there are no errors | a second consumer in the same group, for example a manual `python -m recon.loader --group recon-airflow-loader` |
| Lag is 0 but the data is still old | the producer side (the source system) stopped sending: talk to its owner |

**Do:**

- Fix the cause, then clear and re-run `load_new_messages` for today. Its load is bounded (it stops
  at the end of each partition) and idempotent.
- **Re-run every date that ran while the data was late.** Their windows may have closed on missing
  data, which shows up as `missing_settlement` exceptions that are not real. The re-run's
  `recheck_pending` records them as settled once the data is in.
- Never reset the consumer group's offsets forward to "skip" lag: that drops data. Resetting backward
  (a replay) is always safe.

---

## 4. Agent quality drop

**You see:** reviewers reject more proposals, more diagnoses fall back to escalation, or the policy
overrides (low confidence, escalation floor) jump. The safety guarantees still hold whatever the model
does: the safety contract, the escalation floor and human approval are all in code.

**Check the audit log** (DuckDB `data/copilot.duckdb`; on Snowflake the same table in `RECON.COPILOT`,
read as the loader):

```sql
SELECT CAST(created_at AS DATE) AS day,
       count(*) FILTER (WHERE event_type = 'diagnosis') AS diagnoses,
       count(*) FILTER (WHERE event_type = 'diagnosis'
                        AND CAST(json_extract(payload, '$.is_fallback') AS BOOLEAN)) AS fallbacks,
       count(*) FILTER (WHERE event_type = 'policy_override') AS overrides,
       count(*) FILTER (WHERE event_type = 'review'
                        AND json_extract_string(payload, '$.decision') = 'rejected') AS rejected,
       count(*) FILTER (WHERE event_type = 'review') AS reviews
FROM audit_log GROUP BY 1 ORDER BY 1;
```

Then find what changed on the day the numbers moved:

- **Model or prompt:** each run logs `model` and `prompt_version`. Did `RECON_MODEL` change, or the
  prompt (`PROMPT_VERSION` in [prompts.py](../src/recon/copilot/prompts.py))?
- **Fallback reasons:** `json_extract_string(payload, '$.fallback_reason')` on `diagnosis` events.
  Timeouts and tool-budget exhaustion point to slow tools or harder cases. Validation failures point
  to a model or prompt problem.
- **Tool errors:** `json_extract_string(payload, '$.error')` on `tool_call` events. A schema change
  upstream (2) can change what the tools return.
- **The data:** is a new kind of exception arriving that no eval case covers? Read the rejected
  proposals' `rationale` with `python -m recon.copilot show <exception_id>`.

**Do:**

1. **Measure it, don't guess.** Re-run the evals on DuckDB. Results resume from the existing file, so
   move it aside first:
   ```bash
   mkdir -p evals/results/archive && mv evals/results/claude-sonnet-5-5__standard.jsonl evals/results/archive/
   .venv/bin/python -m recon.evals estimate
   .venv/bin/python -m recon.evals run --model claude-sonnet-5-5 --mode standard --budget 2.5
   .venv/bin/python -m recon.evals report
   ```
   The run costs about $0.34 (Sonnet 5.5, 30 cases). `--budget` is a cap on the estimated spend
   summed over every results file still in `evals/results/` (about $1.50 today), so set it above that
   plus the run. Compare with the baseline in the README: 0 unsafe, 0 fallbacks.
2. **If a model or prompt change caused it,** pin `RECON_MODEL` back to the previous value and record
   why.
3. **If it's a new pattern,** add planted cases for it to the **dev** set, improve the prompt or add a
   rule against dev only, and confirm on a newly generated held-out set.
4. **Until it's fixed,** stop running `diagnose`, and pause `recon_triage` (or set
   `TRIAGE_MODEL_MODE=fake`), the one scheduled thing that runs the copilot. That is the off switch.
   Reviewers work the queue by hand with `show` and `resolve`. Nothing else depends on the copilot.

---

## 5. Snowflake trial expired (or out of credits)

**You see:** Snowflake tasks in `recon_daily` fail. There are two different messages:

| Error says | Meaning |
|---|---|
| the warehouse cannot be resumed because resource monitor `RECON_MONITOR` exceeded its quota | the 10-credit monthly cap was hit (it suspends at 90%) |
| the account is suspended, the trial has ended, or login fails for both users | the trial is over |

**Out of credits:**

1. Pause `recon_daily`: `cd airflow && astro dev run dags pause recon_daily`.
2. An admin either waits for the monthly reset or raises the cap:
   `ALTER RESOURCE MONITOR RECON_MONITOR SET CREDIT_QUOTA = <n>;`.
3. Unpause, then re-run the missed dates (safe).
4. Look at what used the credits: `AUTO_SUSPEND = 60` should keep an idle warehouse at 0.

**Trial expired: keep running on DuckDB.** Everything except the Airflow DAG runs locally:

1. Pause `recon_daily`. It runs on Snowflake only.
2. Unset `WAREHOUSE_ENGINE` (or set it to `duckdb`). Then
   `make load && make recon`, and the copilot and the review CLI work as before against
   `data/warehouse.duckdb` and `data/copilot.duckdb`.
3. **The audit log and resolutions on Snowflake can't be reached once the trial ends.** Export them
   before it ends: run `SELECT * FROM RECON.COPILOT.AUDIT_LOG` and `... RESOLUTIONS` in Snowsight and
   download the results. Also schedule a reminder before the trial end date shown in Snowsight.

**Move to a new account:**

1. Create the account. Run `python snowflake/render_setup.py` (the same key pairs work, or generate
   new ones), then run `secrets/setup_rendered.sql` as ACCOUNTADMIN.
2. Put the new identifier in `SNOWFLAKE_ACCOUNT` in `.env` and in `airflow/` (it reads the same `.env`).
   Then restart Airflow: `make airflow-down && make airflow-up`.
3. `make parity`: reloads RAW from Kafka, builds both engines, and checks they match.
4. `make test-snowflake`: replay, reconciliation and role tests, including that the agent can't write.
5. Rebuild the daily history: `make airflow-backfill FROM=<first day> TO=<today>`.
