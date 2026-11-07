"""The eval harness through the MCP backend (Slice 9e), with the scripted model: no API calls."""

from recon.config import load_settings
from recon.copilot.scripted import ScriptedInvestigator
from recon.evals.runner import label, run_config

VOLATILE = {"run_id", "latency_s", "tool_backend"}


def test_scores_through_mcp_equal_in_process_and_land_in_their_own_file(recon_db, tmp_path):
    runs = {}
    for backend in ("inprocess", "mcp"):
        settings = load_settings({"DUCKDB_PATH": str(recon_db[0]), "GROUND_TRUTH_PATH": str(recon_db[1]),
                                  "COPILOT_DB_PATH": str(tmp_path / "unused.duckdb"), "COPILOT_TOOL_BACKEND": backend})
        runs[backend] = run_config(settings, model="claude-sonnet-5-5", mode="standard", results_dir=tmp_path / "r",
                                   audit_dir=tmp_path / "a", budget_usd=1.0, client=ScriptedInvestigator(), limit=8,
                                   log=lambda line: None)
    assert len(runs["mcp"]) == len(runs["inprocess"]) == 8
    assert all(s.tool_backend == "mcp" for s in runs["mcp"])
    strip = lambda scores: [{k: v for k, v in s.to_dict().items() if k not in VOLATILE} for s in scores]
    assert strip(runs["mcp"]) == strip(runs["inprocess"])
    files = sorted(p.name for p in (tmp_path / "r").glob("*.jsonl"))
    assert files == [f"{label('claude-sonnet-5-5', 'standard', 'copilot-v2')}.jsonl",
                     f"{label('claude-sonnet-5-5', 'standard', 'copilot-v2', 'mcp')}.jsonl"]
    assert files[1].endswith("__copilot-v2__mcp.jsonl")
