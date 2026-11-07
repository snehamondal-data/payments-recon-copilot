# Slice 9 spec: MCP server, automated triage, and MCP evals

**Written 6 Nov 2026.** Upgrade for `payments-recon-copilot`, aimed at FDE roles that ask for agents, MCP and evaluations (e.g. NVIDIA FDE, Finance). **Sneha types the first pass herself;** Claude reviews after. Copy this file into the repo as `docs/SLICE_9_SPEC.md`.

Nothing in this slice weakens an existing guarantee. The agent still only reads, a human still approves every change, and every tool call is still audited.

---

## 0. Order of work (each step ends with tests passing and a commit)

| Step | What | Done when |
|---|---|---|
| 9a | `ToolBackend` interface: the agent loop calls tools through one interface instead of importing the functions directly | All existing tests pass unchanged; in-process backend is the default |
| 9b | MCP server exposing the six read-only tools | Server starts over stdio; `list_tools` shows exactly six tools; contract and security tests pass |
| 9c | MCP client backend for the agent, plus parity check | Every exception gives byte-identical tool output in-process vs via MCP |
| 9d | Automated triage task in Airflow | Fake-model DAG test: new exceptions get proposals, a re-run adds none, nothing is approved |
| 9e | Evals through the MCP path + README | 35-case eval run via MCP, compared with the in-process result; README updated |

Realistic effort typing it yourself: 9a about 1 hour, 9b 2 to 3 hours (including learning the SDK), 9c 1 to 2 hours, 9d 2 to 3 hours, 9e 1 hour.

---

## 1. Why MCP here (be ready to say this)

MCP (Model Context Protocol) is a standard way to expose tools to any AI client. Today the copilot's tools are Python functions only my own agent loop can call. As an MCP server, the same governed tools can be used by any MCP client (Claude Desktop, Claude Code, another team's agent) **without giving that client database credentials or SQL access.** The server is the governance boundary: it holds the least-privilege role, validates every argument, and logs every call.

---

## 2. 9a: the ToolBackend interface

- One small interface with two methods: list the available tools (name, description, input schema) and call a tool by name with arguments, returning the same typed records as today.
- `InProcessBackend` wraps the existing six functions. `McpBackend` comes in 9c.
- The agent loop, validation, escalation floor, fallbacks and audit logging don't change. They only stop caring where the tool runs.
- Config switch: `COPILOT_TOOL_BACKEND = inprocess | mcp` (default `inprocess`).

**Trade-off to explain:** one more layer of indirection, in exchange for testing both paths against each other.

**As built (9a):** the backend sits underneath `ToolSession`, which still tracks calls and returned record IDs, so the evidence check is the same for every backend. `InProcessBackend` owns argument validation. A tool error comes back as data; a backend failure raises `BackendUnavailable`, is recorded on the tool call, and ends the run in the escalate fallback. `Copilot` still reads the warehouse directly for the queue row, the as-of date and the blind-mode flag. **Open for 9c:** should the MCP client open the warehouse at all, or should those three lookups also come from the server?

**Decided in 9c:** it opens none. The queue row comes from `get_exception` over MCP (an orchestrator call, outside the model's budget and its audit trail), the as-of date from an MCP resource, `recon://as-of`, and the blind-mode flag from the client's own settings, with the hiding done by the server. One consequence: in blind mode over MCP the client never learns the reason code, so a fallback diagnosis says `ambiguous` (escalated either way) instead of the reconciliation reason code.

---

## 3. 9b: the MCP server

- **SDK:** the official MCP Python SDK (`mcp` package), its FastMCP server. Read first: modelcontextprotocol.io, the Python SDK quickstart (server, then client).
- **Tools:** exactly the six existing ones: `get_exception`, `get_processor_events`, `get_bank_settlement`, `get_ledger_entries`, `get_related_refunds`, `get_fx_rate`. Same names, same pydantic argument validation, same output. Each MCP tool is a thin wrapper around the existing function. **No write tools, no SQL tool, no `submit_diagnosis` on the server** (the diagnosis is the client's output, validated by the client).
- **Transport:** stdio (the client starts the server as a subprocess). No network port is opened. Remote HTTP transport with auth is out of scope; mention it as the next step.
- **Credentials:** the server opens the warehouse **only** as the agent identity: DuckDB read-only connection, or Snowflake `RECON_AGENT` / `RECON_AGENT_ROLE` with secondary roles off. It must **refuse to start** if given loader credentials, the loader role, or a path to the ground-truth file.
- **Blind mode** works the same: when `COPILOT_HIDE_REASON_CODE` is on, the server's `get_exception` hides `reason_code` and `detected_problems`.
- **Server-side access log:** every call appended as one JSON line (timestamp, tool, arguments, record IDs returned, duration, error) to a local file. This is in addition to the client's audit log in COPILOT. Two independent records of what the agent saw.
- **Tool descriptions** in the MCP listing are written for a model: what the tool returns, what IDs it accepts. Record text is data, never instructions (say so in the server's instructions).

**Trade-offs to explain:**
- *Why not expose SQL through MCP?* Same reason as before: typed tools are safer than generated SQL. MCP doesn't change that.
- *Why does the client write the audit log, not the server?* The server only holds read credentials. Giving it write access to COPILOT would break least privilege. So the server keeps its own local access log, and the client writes the official audit log with the loader role.
- *Why stdio?* Smallest attack surface for a single-machine setup.

---

## 4. 9c: the MCP client backend and parity

- `McpBackend` starts the server over stdio, lists tools, and calls them. It converts MCP results back into the same typed records the agent loop expects.
- If the server fails to start, crashes, or times out, the run ends in the **existing deterministic fallback** (escalate, `is_fallback = true`) with the reason logged. Nothing crashes.
- **Parity check (like the Snowflake parity):** for every exception in the seed-42 scenario, call every relevant tool through both backends and compare outputs byte for byte. Report the count.

---

## 5. 9d: automated triage workflow

A new task at the end of `recon_daily` (or a separate DAG `recon_triage` scheduled after it; pick one and say why):

1. Find exceptions for the logical date that **don't yet have a proposal** for this data version and prompt version.
2. Run the single copilot agent on each, using the MCP backend.
3. Write each diagnosis as a **proposal** with status `proposed`, plus the full audit trail, through the loader role.
4. **Never approve or apply anything.** Escalations land in the queue as escalations.

Rules:
- **Idempotent:** re-running the same date creates zero new proposals.
- **Budget cap per run:** maximum number of exceptions and a maximum estimated API cost; when hit, stop, log it, and leave the rest for the next run.
- **Dry-run mode** with the fake model client, so the DAG can be tested without API calls or cost.
- A summary at the end: exceptions triaged, proposals, escalations, fallbacks, cost.
- **Decision change to document:** before this slice, the copilot was deliberately never scheduled. It's safe to schedule now because the scheduled step only proposes; approval stays human, and the queue shows what was proposed automatically. Add this to "Design decisions" in the README.

---

## 6. 9e: tests and evals for the MCP path

**Tests (no API calls):**
- **Contract:** `list_tools` returns exactly the six tools with the expected input schemas; no tool name contains write, insert, update, delete, sql or query.
- **Arguments:** SQL-injection strings, wrong ID patterns, bad dates and unknown currencies are rejected at the MCP boundary.
- **Least privilege:** the server refuses loader credentials, the loader role and the ground-truth path; on Snowflake (opt-in), the server's session holds exactly `RECON_AGENT_ROLE`.
- **Parity:** in-process vs MCP outputs identical for every exception.
- **Failure modes:** server crash, timeout, malformed result: each ends in the escalate fallback, and is logged.
- **Workflow:** DAG test with the fake model: proposals created only for new exceptions; re-run creates none; nothing approved; budget cap stops processing; audit events and the server access log both present.

**Eval (costs a little):**
- Run the existing 35-case eval (30 + 5 multi-problem) on Sonnet standard, `copilot-v2`, **through the MCP backend.** Compare with the in-process v2 result: same scores expected. Report any difference honestly; differences would come from model variance, so note that.
- Estimated cost: about $0.40 to $0.50. Report the actual figure.

---

## 7. Résumé line (only after 9a to 9e are built and the eval ran)

Draft, to fill in with measured numbers:
> Exposed the copilot's typed, read-only tools as an MCP server under a least-privilege Snowflake role, and added an Airflow workflow in which a single agent triages new exceptions into a human-approval queue; verified MCP and in-process tool outputs identical across N exceptions and re-ran the 35-case eval through MCP (result).

❌ Never claim: multi-agent, autonomous fixes, production use, or an MCP deployment beyond local stdio.

---

## 8. Rules (same as the project's §7)
- Synthetic data only. No em dashes in README or docs. One commit per step. No secrets committed.
- First pass is Sneha's, typed by hand. Use Claude Code or this chat for review after each step, not to write the step.
