# Project spec: Payments Reconciliation Copilot

**Working repo name:** `payments-recon-copilot`
**Written 2 Nov 2026.** Personal project, built to show forward-deployed engineering (FDE) skills for AI engineering, FDE and data engineering roles.
**Build with Claude Code, one slice at a time.** Each slice must work end to end and be committed before the next one starts.

---

## 1. The story (what this project proves)

I play a **forward deployed engineer** sent to a customer, a **fictional** payments startup called **Tallyline**. Its operations team reconciles money by hand: what the **payment processor** says happened, what the **bank** actually settled, and what the **internal ledger** recorded. The three disagree every day. My job is to:

1. take their messy, streaming data and reconcile it reliably,
2. surface the **exceptions** (what doesn't tie out) in a queue,
3. deploy an **AI copilot** that investigates each exception and proposes a fix, **with a human approving every change**, and
4. prove it works (evals), keep it safe (least privilege), and hand over a rollout plan and runbook.

**Everything is synthetic.** Tallyline is fictional, and all data is generated. Say so at the top of the README.

---

## 2. Customer discovery (seed for `docs/DISCOVERY.md`)

- **Who:** Tallyline's 4-person payments-ops team.
- **Today:** they export three CSVs every morning and match them in spreadsheets. About 2–3% of transactions don't tie out. Each one takes 10–20 minutes to investigate. (These are fictional figures for the scenario; label them as such.)
- **Pain:** a growing backlog, late discovery of real problems (missing money), and no audit trail of why something was resolved.
- **Success metrics we agree on:**
  - exceptions backlog and age
  - **% of exceptions where the copilot's diagnosis is accepted by a human**
  - median time to resolve
  - **zero** changes applied without human approval

---

## 3. The data and the mess (planted problems = ground truth for evals)

Three sources, each published to its own Kafka topic:

| Source | Topic | Records |
|---|---|---|
| Payment processor | `processor.events` | charges, refunds (full/partial), fees, disputes/chargebacks |
| Bank | `bank.settlements` | settlement batches, **net of fees**, arriving **T+1 or T+2** |
| Internal ledger | `ledger.entries` | double-entry postings by the company's own system |

**Planted exception types.** Each is generated with a known cause, so evals can score the agent:

1. **Late settlement:** not settled yet, but still inside the T+2 window. **Not a real exception;** the system must not flag it early.
2. **Missing settlement:** past the window and never settled.
3. **Duplicate webhook:** the processor sent the same event twice.
4. **Fee mismatch:** the bank's net amount ≠ gross − the expected fee.
5. **Partial refund** not reflected in the ledger.
6. **Currency rounding:** FX conversion off by a cent or two.
7. **Missing ledger entry:** the processor and bank agree, but the ledger has nothing.
8. **Orphan bank deposit:** money arrived with no matching charge.
9. **Chargeback:** money pulled back after settlement.
10. **Date boundary:** UTC vs local time puts a transaction on different days in different sources.
11. **Ambiguous case:** two plausible explanations. **The correct agent behaviour is to escalate, not guess.**

---

## 4. Architecture

```
generator (Python) ──► Kafka topics (+ dead-letter topic)
                          │
                          ▼
              consumer / loader (Python): validates each message against a contract,
              bad messages → DLQ, idempotent load (dedupe on event_id; safe to replay)
                          │
                          ▼
              warehouse: DuckDB (local default) or Snowflake (DB_ENGINE switch)
                          │
                          ▼
              dbt: staging → matching → marts
                   recon_matches, recon_exceptions (reason code, amount, age, window status)
                          │
                          ▼
              copilot agent (Anthropic SDK): typed read-only tools → structured diagnosis
                          │
                          ▼
              review CLI: proposed → approved / rejected → applied   (+ audit log)
                          │
                          ▼
              metrics: backlog, acceptance rate, time to resolve
              (later) Airflow DAG orchestrates the scheduled parts
```

### Key design decisions (be ready to defend these)

- **Real Apache Kafka** in Docker (KRaft mode, no ZooKeeper). Redpanda would be simpler, but the point is to learn and show Kafka.
- **Message contracts:** pydantic models per topic. Invalid messages go to `dlq` with the reason; nothing crashes.
- **Idempotent, replayable loads:** replaying a topic from the beginning inserts **zero** duplicate rows. Dedupe on `event_id`, and commit offsets only after a successful load.
- **Time windows:** reconciliation respects T+2 settlement. Status values: `matched`, `pending_in_window`, `exception`. A missing settlement only becomes an exception after its window closes.
- **Dual engine:** the **same dbt models** run on `dbt-duckdb` (local, default) and `dbt-snowflake` (trial), switched by profile target. The Python side uses one small warehouse-client interface with two implementations. Keep it minimal; no heavy abstraction.
- **Agent safety:**
  - **The agent never writes SQL.** It calls **typed, tested tool functions** that run parameterised read-only queries:
    - `get_exception(id)`
    - `get_processor_events(txn_id)`
    - `get_bank_settlement(txn_id)`
    - `get_ledger_entries(txn_id)`
    - `get_related_refunds(txn_id)`
    - `get_fx_rate(date, currency)`
  - In Snowflake, the agent's user has a **read-only role** on the marts only (least privilege).
  - **Structured output** (pydantic): `root_cause` (enum), `evidence` (record IDs), `proposed_action` (enum), `confidence`, `needs_human_escalation` (bool). Invalid output gets one retry, then a **deterministic fallback**: "escalate to human".
  - The agent **only proposes.** Applying means writing a **resolution record**. Source data is never changed.
  - **Audit log table:** exception ID, tools called and arguments, model, output, reviewer decision, and timestamps.
- **Model:** Anthropic SDK (same as her other agent). API key from an environment variable only; never committed.

---

## 5. Evals

- **At least 20 planted cases** covering every type in §3, including **late settlement (must NOT be flagged)** and **ambiguous (must escalate)**.
- **Scored per case:** correct `root_cause`; evidence includes the right record IDs; a safe `proposed_action`; escalates when it should.
- **Hard rule:** count **unsafe proposals** (a wrong action on a case that should escalate). The target is 0, reported separately.
- **Report the score honestly, misses included.** Don't tune prompts against the eval set until it scores 100%; keep a few held-out cases.

---

## 6. Slices (build order). Each ends with tests passing and a commit.

**Slice 1: data in.**
- Docker Compose with Kafka (KRaft).
- A generator that publishes the three sources with all planted problems, using a fixed random seed.
- pydantic contracts, a DLQ, and an idempotent loader into DuckDB.
- **Done when:** a replay inserts zero duplicate rows, bad messages land in the DLQ, and pytest passes.

**Slice 2: reconciliation.**
- dbt on DuckDB: staging, matching, `recon_matches` and `recon_exceptions`, with reason codes and T+2 windows.
- dbt tests: grain/uniqueness, matched totals reconcile, and **late settlements are not exceptions**.
- **Done when:** every planted problem appears with the right reason code, and the late settlement doesn't.

**Slice 3: the copilot.**
- The typed tools, the agent loop, structured output, fallbacks, the review CLI (approve or reject, with a reason) and the audit log.
- **Done when:** a human can work through the queue end to end, and every step is in the audit log.

**Slice 4: evals and metrics.**
- The eval runner and report: backlog, acceptance rate and time to resolve.
- **Done when:** the eval report is in the README, with the honest score and the unsafe-proposal count.

**Slice 5: Snowflake.**
- ⚠️ **Ask her before starting:** the trial is time-limited.
- The `dbt-snowflake` target, a read-only role for the agent, the same models and tests passing.
- Record a **2-minute demo video** while the trial is active.
- **Done when:** the same results come out on both engines.

**Slice 6: Airflow.**
- An Astro CLI project, with a DAG that runs dbt, re-checks pending settlements daily as the windows close, and runs evals nightly.
- **Done when:** the DAG runs locally, end to end.

**Slice 7: FDE documents.**
- `docs/DISCOVERY.md` (§2), `docs/ROLLOUT.md` (shadow mode, then assisted, then measured; with go/no-go criteria) and `docs/RUNBOOK.md` (exception spike, schema change upstream, Kafka lag, agent quality drops, Snowflake trial expired).
- A README that opens with "In one minute" in plain English, then the demo video link, then the architecture.

**Slice 8 (optional, for document-AI roles like Reducto): documents.**
- Some bank settlements arrive as **PDF statements** instead of a feed. Generate a few realistic synthetic PDFs: multi-page, tables, and one badly formatted.
- Add an extraction step that turns them into settlement rows (start with an open-source parser; keep the interface swappable).
- Add an **extraction-accuracy eval** (field-level: amount, date, reference) against the known truth.
- **Done when:** the extracted rows flow into the same reconciliation, and the extraction accuracy is reported honestly.

**Slice 9 (optional, for FDE roles that deploy into customer environments): deploy it.**
- Package the loader, the dbt run and the copilot as containers, and deploy them to **local Kubernetes** (kind or minikube) with a simple Helm chart or plain manifests.
- Config and secrets come from Kubernetes objects. Add a `docs/DEPLOY.md` playbook: install, upgrade, roll back and troubleshoot, including notes on running with **no internet access** (what would need to change).
- **Done when:** a fresh cluster can be stood up from the playbook alone.
- **Label it project-scale.** Kubernetes stays off "production" claims.

---

## 7. Rules

- **Synthetic data, clearly labelled.** Never imply a real customer or real money.
- **Résumé honesty:** Kafka, Snowflake and Airflow are **project-scale**, never production. Never claim a score that wasn't measured.
- **No em dashes** in the README or docs (her style rule).
- **One commit per slice,** with clear messages. Don't commit secrets; `.env` goes in `.gitignore`.
- **Mac on Apple Silicon (arm64):** use arm64-compatible images and wheels.
- **Out of scope:** a web UI (the CLI is enough), real bank or processor APIs, AWS and Databricks.

---

## 8. Interview angles this project gives her

- **FDE:** "I scoped a customer problem, agreed success metrics, deployed into their messy data, and planned a shadow-mode rollout."
- **Data engineering:** Kafka contracts and a DLQ, idempotent replay, late data and time windows, dual-engine dbt.
- **AI engineering:** typed tools instead of SQL generation (and why), structured output with fallbacks, least privilege, an audit log, and honest evals including escalation.
- **It ties to real experience:** reconciling Shopify, ShipStation and the warehouse at work, and her SQL reconciliation practice.
