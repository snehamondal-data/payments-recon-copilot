"""The README and docs: the style rule (no em dashes), the README's section order, and links that resolve."""

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SKIP = {".venv", "data", "node_modules", "target", "logs", "dbt_packages"}
MARKDOWN = [p for p in ROOT.rglob("*.md") if not SKIP & set(p.relative_to(ROOT).parts)
            and not any(part.startswith(".") for part in p.relative_to(ROOT).parts)]


def test_markdown_files_were_found():
    names = {p.relative_to(ROOT).as_posix() for p in MARKDOWN}
    assert {"README.md", "docs/DISCOVERY.md", "docs/ROLLOUT.md", "docs/RUNBOOK.md", "airflow/README.md"} <= names


@pytest.mark.parametrize("path", MARKDOWN, ids=lambda p: p.relative_to(ROOT).as_posix())
def test_no_em_dashes(path):
    lines = [i for i, line in enumerate(path.read_text().splitlines(), start=1) if "—" in line]
    assert lines == [], f"em dash on lines {lines}"


def test_readme_sections_are_in_the_agreed_order():
    headings = re.findall(r"^## (.+)$", (ROOT / "README.md").read_text(), flags=re.M)
    assert headings[:7] == ["In one minute", "Architecture", "Results", "Design decisions", "How to run it",
                            "Semantic layer", "How it works"]


def test_readme_opens_with_the_one_minute_summary():
    text = (ROOT / "README.md").read_text()
    minute = text.split("## In one minute", 1)[1].split("##", 1)[0]
    assert "All data is synthetic" in minute
    sentences = re.split(r"(?<=[.!?])\s+(?=[A-Z*])", " ".join(minute.split()))
    assert 5 <= len([s for s in sentences if s]) <= 7
    assert "```mermaid" in text.split("## Results")[0]


def test_readme_results_include_parity_and_the_backfill():
    results = (ROOT / "README.md").read_text().split("## Results", 1)[1].split("## Design decisions")[0]
    for heading in ("### Copilot evals", "### DuckDB vs Snowflake parity", "### Airflow backfill"):
        assert heading in results


def test_design_decisions_has_six_to_eight_bullets():
    section = (ROOT / "README.md").read_text().split("## Design decisions", 1)[1].split("\n## ", 1)[0]
    assert 6 <= len(re.findall(r"^- \*\*", section, flags=re.M)) <= 8


def test_runbook_covers_the_five_incidents():
    headings = re.findall(r"^## \d\. (.+)$", (ROOT / "docs" / "RUNBOOK.md").read_text(), flags=re.M)
    assert [h.split(" ")[0] for h in headings] == ["Exception", "Upstream", "Kafka", "Agent", "Snowflake"]


@pytest.mark.parametrize("path", MARKDOWN, ids=lambda p: p.relative_to(ROOT).as_posix())
def test_relative_links_resolve(path):
    broken = []
    for target in re.findall(r"\]\(([^)\s]+)\)", path.read_text()):
        if target.startswith(("http://", "https://", "mailto:", "#")):
            continue
        if not (path.parent / target.split("#", 1)[0]).exists():
            broken.append(target)
    assert broken == []
