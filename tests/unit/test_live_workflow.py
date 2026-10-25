"""The live workflow requires explicit opt-in and never runs privileged code from PRs."""

from pathlib import Path

import yaml


def test_live_workflow_is_opt_in_bounded_and_uses_only_runtime_dependencies():
    path = Path(__file__).resolve().parents[2] / ".github/workflows/live-inference.yml"
    config = yaml.load(path.read_text(), Loader=yaml.BaseLoader)
    assert set(config["on"]) == {"schedule", "workflow_dispatch"}
    assert config["permissions"] == {"contents": "read"}
    assert config["concurrency"]["cancel-in-progress"] == "false"
    job = config["jobs"]["refresh"]
    assert job["if"] == "vars.LIVE_INFERENCE_ENABLED == 'true'"
    assert int(job["timeout-minutes"]) <= 20
    commands = "\n".join(step.get("run", "") for step in job["steps"])
    assert "--require-hashes -r requirements.lock" in commands
    assert "firewatch.scheduled_worker --max-jobs 3" in commands
    assert "seed-regions" not in commands and "alembic" not in commands
    assert not any("upload-artifact" in step.get("uses", "") for step in job["steps"])
