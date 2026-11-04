"""The semantic layer: five metrics defined in dbt (MetricFlow), queried only through typed requests.

The metric definitions live in dbt/models/semantic/_semantic.yml. dbt compiles them into
target/semantic_manifest.json; MetricFlow turns a typed MetricQuery into SQL from that manifest; and the
SQL runs on the same locked-down, read-only DuckDB connection the copilot's tools use. Nothing here
accepts SQL, and every filter value is checked against a closed set before it is rendered.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from recon.copilot.contract import RootCause
from recon.copilot.tools import open_read_only

Metric = Literal["exception_count", "amount_at_risk", "open_backlog", "pending_count", "match_rate"]
Dimension = Literal["reason_code", "window_status", "currency", "as_of_date"]
METRICS: tuple[str, ...] = Metric.__args__
DIMENSIONS: tuple[str, ...] = Dimension.__args__

# Our dimension names -> MetricFlow group-by names (entity `item`; as_of_date is the metric time).
GROUP_BY = {"reason_code": "item__reason_code", "window_status": "item__window_status",
            "currency": "item__currency", "as_of_date": "metric_time__day"}
ALLOWED_VALUES = {"reason_code": {c.value for c in RootCause},
                  "window_status": {"matched", "pending_in_window", "exception"},
                  "currency": {"USD", "EUR", "GBP"}}
DIMENSION_HELP = {
    "reason_code": "why an exception doesn't tie out (one of the 10 reason codes); empty for matched and pending items",
    "window_status": "matched, pending_in_window (unsettled but still inside T+2) or exception",
    "currency": "the charge's original currency: USD, EUR or GBP (all amounts are in USD)",
    "as_of_date": "the date the reconciliation was run for (YYYY-MM-DD)",
}
MAX_ROWS = 50


class QueryError(Exception):
    """Returned to the model as an is_error tool result."""


class _Args(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Filter(_Args):
    dimension: Dimension
    op: Literal["=", "!=", "in", "not in"]
    values: Annotated[list[str], Field(min_length=1, max_length=12)]

    @model_validator(mode="after")
    def _check(self) -> Filter:
        if self.op in ("=", "!=") and len(self.values) != 1:
            raise ValueError(f"'{self.op}' takes exactly one value")
        for v in self.values:
            if self.dimension == "as_of_date":
                date.fromisoformat(v)  # raises for anything that isn't a real date
            elif v not in ALLOWED_VALUES[self.dimension]:
                raise ValueError(f"{v!r} is not a {self.dimension}; allowed: {sorted(ALLOWED_VALUES[self.dimension])}")
        return self

    def where(self) -> str:
        """A MetricFlow where clause. Safe to render: every value was checked against a closed set above."""
        ref = ("{{ TimeDimension('metric_time', 'day') }}" if self.dimension == "as_of_date"
               else f"{{{{ Dimension('{GROUP_BY[self.dimension]}') }}}}")
        if self.op in ("=", "!="):
            return f"{ref} {self.op} '{self.values[0]}'"
        return f"{ref} {self.op.upper()} ({', '.join(repr(v) for v in self.values)})"


class MetricQuery(_Args):
    metric: Metric = Field(description="One of the defined metrics.")
    dimensions: Annotated[list[Dimension], Field(max_length=2, description="Break the metric down by these.")] = []
    filters: Annotated[list[Filter], Field(max_length=3, description="Keep only matching items.")] = []

    @model_validator(mode="after")
    def _unique(self) -> MetricQuery:
        if len(set(self.dimensions)) != len(self.dimensions):
            raise ValueError("each dimension at most once")
        return self


@dataclass(frozen=True)
class MetricInfo:
    name: str
    label: str
    description: str
    unit: str  # "USD" | "count" | "fraction"


class _CompileOnlyClient:
    """MetricFlow needs a SqlClient for the dialect; this one only compiles. Execution is ours, read-only."""

    def __init__(self):
        from metricflow.protocols.sql_client import SqlEngine
        from metricflow.sql.render.duckdb_renderer import DuckDbSqlPlanRenderer

        self.sql_engine_type = SqlEngine.DUCKDB
        self.sql_plan_renderer = DuckDbSqlPlanRenderer()

    def query(self, *args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("compile-only client: SQL runs on the read-only connection")

    execute = dry_run = query

    def close(self) -> None:
        pass

    def render_bind_parameter_key(self, key: str) -> str:
        return f"${key}"


def _format(value: Any, unit: str) -> Any:
    if value is None:
        return None
    if unit == "USD":
        return str((Decimal(int(value)) / 100).quantize(Decimal("0.01")))  # exact: the measure sums cents
    if unit == "fraction":
        return round(float(value), 4)
    return int(value)


class SemanticLayer:
    def __init__(self, manifest_path: Path, warehouse_path: Path, *, forbidden_paths: tuple[Path, ...] = ()):
        from metricflow.engine.metricflow_engine import MetricFlowEngine
        from metricflow_semantics.model.dbt_manifest_parser import parse_manifest_from_dbt_generated_manifest
        from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup

        warehouse_path, manifest_path = Path(warehouse_path), Path(manifest_path)
        for forbidden in forbidden_paths:
            if warehouse_path.resolve() == Path(forbidden).resolve():
                raise ValueError(f"refusing to open {warehouse_path}: it is not the warehouse")
        if not manifest_path.exists():
            raise FileNotFoundError(f"{manifest_path} not found: run `make recon` (dbt build) first")
        self.manifest = parse_manifest_from_dbt_generated_manifest(manifest_path.read_text())
        # The compiled SQL names the DuckDB catalog the manifest was built against, which is the file's stem.
        built_for = self.manifest.semantic_models[0].node_relation.database
        if built_for != warehouse_path.stem:
            raise ValueError(f"the semantic manifest was built for '{built_for}', not {warehouse_path.name}")
        self._engine = MetricFlowEngine(semantic_manifest_lookup=SemanticManifestLookup(self.manifest),
                                        sql_client=_CompileOnlyClient())
        self.con = open_read_only(warehouse_path)
        self.catalog = {m.name: MetricInfo(
            m.name, m.label or m.name, " ".join((m.description or "").split()),
            {"usd_cents": "USD"}.get((m.config.meta or {}).get("unit") if m.config else None,
                                     "fraction" if m.type.value == "ratio" else "count"))
            for m in self.manifest.metrics
            if not ((m.config.meta or {}).get("internal") if m.config else False)}
        assert set(self.catalog) == set(METRICS), (set(self.catalog), METRICS)

    def close(self) -> None:
        self.con.close()

    def compile(self, q: MetricQuery) -> str:
        from metricflow.engine.metricflow_engine import MetricFlowQueryRequest

        request = MetricFlowQueryRequest.create(
            metric_names=[q.metric], group_by_names=[GROUP_BY[d] for d in q.dimensions] or None,
            where_constraints=[f.where() for f in q.filters] or None)
        return self._engine.explain(request).sql_statement.sql

    def query(self, q: MetricQuery) -> dict[str, Any]:
        started = time.monotonic()
        cur = self.con.execute(self.compile(q))
        names = [d[0] for d in cur.description]
        back = {v: k for k, v in GROUP_BY.items()}
        info = self.catalog[q.metric]
        rows = []
        for raw in cur.fetchall():
            rec = dict(zip(names, raw))
            row = {back[n]: (v.isoformat() if isinstance(v, date) else v) for n, v in rec.items() if n in back}
            row["value"] = _format(rec[q.metric], info.unit)
            rows.append(row)
        rows.sort(key=lambda r: tuple("" if r.get(d) is None else str(r[d]) for d in q.dimensions))
        return {"metric": q.metric, "unit": info.unit, "dimensions": q.dimensions,
                "filters": [f.model_dump() for f in q.filters], "rows": rows[:MAX_ROWS],
                "truncated": len(rows) > MAX_ROWS, "duration_ms": round((time.monotonic() - started) * 1000, 1)}

    def catalog_text(self) -> str:
        lines = ["Metrics:"]
        lines += [f"- {m.name} ({m.unit}): {m.description}" for m in self.catalog.values()]
        lines += ["Dimensions (break down or filter by):"]
        lines += [f"- {d}: {DIMENSION_HELP[d]}" for d in DIMENSIONS]
        return "\n".join(lines)
