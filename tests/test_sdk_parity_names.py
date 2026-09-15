"""SDK type-name parity ratchet.

Python and TypeScript are independent implementations of shared behavioral
contracts (see ARCHITECTURE.md), but their shared vocabulary is checked in.
This test locks the set of contract type names that exist on both sides so
that drift (a rename on one side only, or silent new divergence) must be
reflected consciously in the baseline.

Cross-language naming drift is registered explicitly in the baseline's
``drift`` mapping; both names must exist on their respective sides.
"""

import ast
import json
import re
from pathlib import Path

CORE_DIR = Path(__file__).resolve().parents[1] / "src" / "purra"
TS_DIR = Path(__file__).resolve().parents[1] / "typescript" / "src"
BASELINE = Path(__file__).resolve().parents[1] / "docs" / "sdk-parity-names.json"


def python_contract_names() -> set[str]:
    names: set[str] = set()
    for path in sorted(CORE_DIR.glob("contracts/*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in tree.body:
            if isinstance(node, ast.ClassDef):
                names.add(node.name)
    return names


def typescript_type_names() -> set[str]:
    names: set[str] = set()
    for path in sorted(TS_DIR.rglob("*.ts")):
        source = path.read_text(encoding="utf-8")
        for match in re.finditer(
            r"export\s+(?:interface|type|class|enum)\s+(\w+)", source
        ):
            names.add(match.group(1))
    return names


def test_parity_names_match_the_registered_baseline():
    baseline = json.loads(BASELINE.read_text(encoding="utf-8"))
    registered = set(baseline["common"])
    observed = python_contract_names() & typescript_type_names()

    removed = sorted(registered - observed)
    added = sorted(observed - registered)
    assert not removed, (
        "Shared SDK type names disappeared; update "
        f"docs/sdk-parity-names.json consciously: {removed}"
    )
    assert not added, (
        "New shared SDK type names appeared; register them in "
        f"docs/sdk-parity-names.json: {added}"
    )


def test_documented_cross_language_drift_pairs_exist():
    baseline = json.loads(BASELINE.read_text(encoding="utf-8"))
    py = python_contract_names()
    ts = typescript_type_names()
    for python_name, ts_name in baseline.get("drift", {}).items():
        assert python_name in py, f"drift baseline references missing Python type {python_name}"
        assert ts_name in ts, f"drift baseline references missing TypeScript type {ts_name}"
