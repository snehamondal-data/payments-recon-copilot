# Discovery: Tallyline payments operations

> **Fictional customer, fictional figures.** Tallyline is a made-up payments startup, and every number
> on this page describing its team or workload is invented for the scenario. Nothing here describes a
> real company. Figures measured by this project are marked **measured** and link to where they come from.

This is the write-up a forward deployed engineer would leave after the first week with the customer:
who does the work today, what hurts, which success metrics we agreed, and what we chose not to build.

## Who

Tallyline's **payments operations team: 4 people** (fictional). They own the daily close: making sure
every customer payment the processor reports turns into money in the bank and the right entries in the
ledger. Finance relies on their sign-off; engineering owns the three source systems but not the
reconciliation.

## How it works today

- **Every morning, three CSV exports:** the payment processor's events (charges, fees, refunds,
  disputes), the bank's settlement statement, and the internal ledger's journals.
- **Matching in spreadsheets,** by charge ID where the bank reference has one, by amount and date
  where it doesn't.
- **About 2 to 3% of transactions don't tie out** (fictional). Each one takes **10 to 20 minutes** to
  investigate (fictional): find the charge in each export, work out which source is wrong, decide the fix.
- **Fixes are made by hand** in the ledger, with the reason kept, if at all, in a spreadsheet comment.

## What hurts

| Pain | What it looks like | Why it matters |
|---|---|---|
| A growing backlog | Exceptions from busy days roll into the next morning | Old exceptions are harder to explain, and the close slips |
| Late discovery of real problems | A missing settlement looks like "the bank is just slow" until weeks later | Real money goes missing quietly |
| No audit trail | Nobody can say why an exception was closed, or who decided | Finance and auditors can't rely on the reconciliation |
| Timing noise | A payment made late in the evening lands on the next day in the ledger | Reviewers waste time on items that aren't wrong |

## What we heard about the data

These shaped the design more than anything else:

- **Settlement is T+2 business days.** An unsettled charge is not an exception until its window has
  closed. Flagging it earlier is the biggest source of noise.
- **The processor sometimes sends the same webhook twice** with a new event ID. That is a real
  business duplicate, unlike Kafka redelivering the same event, which must be ignored.
- **Some bank lines arrive with no reference.** They can only be matched on amount and date, and
  sometimes more than one charge fits.
- **The ledger books in local time,** the processor and bank in UTC.
- **Upstream formats change without warning,** and a bad export used to break the morning.

## Success metrics we agreed

| Metric | How this project measures it | Where |
|---|---|---|
| Exceptions backlog and age | count and `age_business_days` in `recon_exceptions`, per as-of date | `recon_daily` summary, `MARTS.RECON_DAILY_RUNS` |
| **% of copilot diagnoses accepted by a human** | approvals over reviewed proposals, from the audit log | `review_metrics()` in [review.py](../src/recon/evals/review.py); `python -m recon.evals metrics --run ...` for an eval run |
| Median time to resolve | first diagnosis to resolution, from the audit log | same |
| **Zero changes applied without human approval** | every resolution record has a reviewer decision | same; the copilot only ever proposes |

**Baseline and what we have measured so far** (all on synthetic data):

- Reconciliation finds **all 30 planted exceptions** with the right reason code and no false alarms,
  and holds the 3 late settlements as pending (**measured**, [README Results](../README.md#reconciliation)).
- The copilot's diagnoses were accepted in **27 of 27** reviewed cases in a single scripted demo
  review session, with **0** changes applied without approval (**measured**, but a demo reviewer, not a
  real team; see [README](../README.md#operational-metrics-a-single-demo-review-session)).
- Time to resolve for a real team is **not measured**. That is what the measured phase of the
  [rollout](ROLLOUT.md) is for.

## Constraints we agreed

- **A human approves every change.** The copilot proposes; approving writes a resolution record and
  never edits source data.
- **Least privilege.** The copilot reads only what it needs and can write nothing.
- **Explainable.** Every proposal cites the records it is based on, and every step is in an audit log.
- **No new UI.** A command-line review queue is enough for a 4-person team at first.

## Out of scope

- Real bank or processor APIs: the project uses generated data published to Kafka.
- A web UI, and cloud deployment beyond a Snowflake trial (no AWS, no Databricks).
- Automatic fixes of any kind.

## Open questions for the customer

- Which bank holidays should the settlement window skip? (Weekends only today.)
- What is the materiality threshold below which a 1 or 2 cent FX difference can be closed in bulk?
- Who besides the ops team should see escalated ambiguous deposits: finance, or the processor's
  support desk?
