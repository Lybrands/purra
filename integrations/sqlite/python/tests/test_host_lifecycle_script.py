import json
import importlib.util
from pathlib import Path
import subprocess
import sys

import pytest


def test_host_lifecycle_script_reconstructs_and_stops(tmp_path):
    script = Path(__file__).resolve().parents[1] / "scripts/check_host_lifecycle.py"
    result = subprocess.run([sys.executable, str(script), "--directory", str(tmp_path / "fixture")],
        check=True, capture_output=True, text=True, timeout=20)
    report = json.loads(result.stdout)
    assert report == {
        "schemaVersion": 1, "runStatus": "done", "toolCalls": 1,
        "modelCalls": 2, "workerScans": report["workerScans"],
        "workerStopped": True, "hostConfigurationReconstructed": True,
        "syntheticOnly": True,
    }
    assert report["workerScans"] >= 2


def test_host_registry_rejects_changed_current_binding(tmp_path):
    script = Path(__file__).resolve().parents[1] / "scripts/check_host_lifecycle.py"
    spec = importlib.util.spec_from_file_location("host_lifecycle_example", script)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    path = tmp_path / "host-runs.json"
    registry = module.HostRegistry(path)
    registry.save("run", expires_at_ms=123)
    saved = json.loads(path.read_text())
    saved["run"]["bindingRevision"] = "2"
    path.write_text(json.dumps(saved))
    with pytest.raises(ValueError, match="host_run_configuration_mismatch"):
        registry.resolve("run")
