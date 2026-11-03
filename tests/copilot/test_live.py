"""One real call to Claude (costs a few cents). Opt in with `make test-live` (pytest -m live).

It checks that the real model, through the real SDK, produces a diagnosis that passes the safety
contract without a fallback. It does not score accuracy: that is Slice 4's eval.
"""

import os

import pytest
from dotenv import load_dotenv

from recon.config import load_settings
from recon.copilot.__main__ import render_show
from recon.copilot.agent import AgentConfig, Copilot
from recon.copilot.contract import is_allowed
from recon.copilot.model_client import AnthropicModelClient
from recon.copilot.store import CopilotStore
from recon.copilot.tools import ReadOnlyWarehouse

pytestmark = pytest.mark.live


def test_live_diagnosis_passes_the_contract(recon_db, cases, tmp_path):
    load_dotenv()
    if not os.environ.get("ANTHROPIC_API_KEY"):
        pytest.fail("the live test needs ANTHROPIC_API_KEY in .env (it was selected with -m live)")
    settings = load_settings()
    case = cases["fee_mismatch"]  # a dev-set case, never a held-out one
    warehouse = ReadOnlyWarehouse(recon_db[0], forbidden_paths=(recon_db[1],))
    store = CopilotStore(tmp_path / "copilot.duckdb")
    try:
        client = AnthropicModelClient(settings.recon_model, effort=settings.copilot_effort)
        run = Copilot(client, warehouse, AgentConfig(max_tool_calls=settings.copilot_max_tool_calls,
                                                     timeout_s=settings.copilot_timeout_s)).diagnose(case.exception_id)
        store.record_run(run)
        print("\n" + render_show(warehouse, store, case.exception_id))
        print(f"\nserved by: {run.served_models}   validation failures: {run.validation_failures}")
        assert not run.is_fallback, run.fallback_reason
        d = run.diagnosis
        assert is_allowed(d.root_cause, d.proposed_action)
        assert set(d.evidence) <= {rid for c in run.tool_calls for rid in c.record_ids}
        assert 1 <= len(run.tool_calls) <= settings.copilot_max_tool_calls
    finally:
        store.close()
        warehouse.close()
