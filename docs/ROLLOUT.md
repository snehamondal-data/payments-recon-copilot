# Rollout plan: shadow, then assisted, then measured

> **A plan for a fictional customer.** Tallyline and its team are made up, and this rollout has not been
> run. The thresholds below are the go/no-go criteria we would agree with the customer; where the
> project already has a measured number (on synthetic data), it is quoted as the starting point.

The copilot never applies anything. In every phase, a human approves every change, and approving only
writes a resolution record. What changes from phase to phase is **who sees the proposals and what we
measure**. Reconciliation (Kafka, the loader, dbt, the daily Airflow run) is already in place before
phase 1 and does not depend on the copilot at all.

## Before phase 1

| Must be true | Why | Status |
|---|---|---|
| A **new held-out set that includes ambiguous cases** (a new seed) | the current held-out set has none, and it has been seen | to do |
| The model and prompt are pinned (`RECON_MODEL`, `PROMPT_VERSION`) and logged on every run | so a quality change can be traced to a cause | done |
| 0 unsafe proposals on the eval set **with the escalation floor** | the floor is the safety guarantee, not the model | measured: 0 of 30 (the raw model: 1 of 30) |
| The agent role can't write anything, and the audit log is written by another role | least privilege | done, tested on Snowflake |
| The runbook has been walked through once with the team | they own the incidents | [RUNBOOK.md](RUNBOOK.md) |

## Phase 1: shadow mode (2 weeks)

**What happens.** After each daily run, an operator runs `python -m recon.copilot diagnose --all`.
The proposals go to a **separate store** that reviewers do not look at. On DuckDB that is
`COPILOT_DB_PATH=data/shadow.duckdb`; on Snowflake it would need a second schema, which is not built
yet. The team resolves exceptions exactly as today, recording each decision with `resolve`, so their
decisions are captured in the same shape. At the end of each week we compare the two. The comparison
script is the first thing to build in this phase.

**What we learn.** How often the copilot agrees with the team, the team's own time to resolve (our
baseline, which is not measured yet), and the cost and latency at real volume.

| Go to phase 2 if all of these hold | Threshold |
|---|---|
| Unsafe proposals (a non-escalate action not allowed for the team's root cause, or a missed escalation) | **0** over the 2 weeks |
| Every exception reconciliation called `ambiguous` was escalated | 100% (the floor makes this automatic) |
| Root cause agrees with the team, non-ambiguous exceptions | at least 90% (evals: 27 of 27 per run) |
| Fallback rate (no valid diagnosis) | at most 5% (evals: 0%) |
| Audit log complete: every run has run_started, tool calls and a diagnosis | 100% |
| Cost per exception | at most $0.05 (evals: about $0.011 with Sonnet 5.5) |
| Median time per diagnosis | at most 30s (evals: about 5s) |

**No-go, stop and fix:** any unsafe proposal. We find out why, add it to the eval set as a dev case,
and change the prompt or add a rule in code. Then we re-run the evals and restart the two weeks.

## Phase 2: assisted (4 weeks)

**What happens.** Reviewers work from the copilot's queue: `queue` shows each exception with its
proposal, and `show` shows the evidence and the full tool trace. They `approve`, `reject` with a reason,
or `resolve` with their own action. Escalations always go to a person. Nothing is applied without an
approval, and the code has no path that applies anything automatically.

| Go to phase 3 if all of these hold | Threshold |
|---|---|
| Changes applied without human approval | **0** (checked daily against the audit log) |
| Acceptance rate: proposals approved over proposals reviewed | at least 80% |
| Median time to resolve, versus the phase 1 baseline | at least 30% faster |
| Resolutions reopened later because they were wrong | no more than in phase 1 |
| Rejection reasons | read every week, and every recurring one becomes an eval case |
| The team says they trust the evidence | yes, at the end of week 4 |

**Roll back to shadow if** an unsafe proposal is approved, the acceptance rate falls below 60% for a
week, or the audit log has a gap. Rolling back means not running `diagnose`. The queue, the dbt
reconciliation and the review CLI keep working without the copilot.

## Phase 3: measured (ongoing)

**What happens.** The team works as in phase 2, and we report the agreed metrics every week against
the phase 1 baseline:

- exceptions backlog and age (the daily run's summary, `MARTS.RECON_DAILY_RUNS`);
- acceptance rate and median time to resolve (from the audit log);
- changes without approval, which must stay at **0**;
- cost per exception, fallback rate and policy overrides.

We also re-run the evals on a schedule (`recon_nightly_evals`, budget-capped, weekly rather than
nightly to start) on the newest held-out set.

| Keep going if | Investigate (see the runbook) if |
|---|---|
| backlog age trends down or holds | backlog grows for 3 days running |
| acceptance rate stays within 5 points of phase 2 | acceptance drops by more than 10 points in a week |
| eval results hold, 0 unsafe | any unsafe case, or recall drops on any type |

## What we will not do, even if the numbers are good

- **Auto-apply fixes.** The 0-unapproved-changes metric is a requirement, not a target to trade off.
- **Lift the escalation floor.** Ambiguous items stay with people.
- **Widen the agent's role.** The copilot keeps read-only access; resolutions are written under a
  different role.

## Who decides

Go/no-go at the end of each phase is a joint call between the Tallyline payments-ops lead (owner of
the process), finance (owner of the ledger) and the forward deployed engineer (owner of the system),
made on the numbers in this document.
