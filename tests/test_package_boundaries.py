"""Package-level ratchets for the independently packaged PurrA framework."""

from __future__ import annotations

import ast
import tomllib
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
PORTABLE_CONFORMANCE = ROOT_DIR / "tests" / "test_standalone_agent_conformance.py"
SECOND_HOST_CONFORMANCE = ROOT_DIR / "tests" / "test_second_host_conformance.py"
HOST_ADAPTER_CONFORMANCE = ROOT_DIR / "src" / "purra" / "testing.py"
PACKAGE_READMES = (ROOT_DIR / "README.md", ROOT_DIR / "README.zh-CN.md")


def _imports(path: Path) -> tuple[str, ...]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    names: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.append(node.module)
    return tuple(names)


def test_purra_is_an_independent_dependency_free_distribution():
    metadata = tomllib.loads(
        (ROOT_DIR / "pyproject.toml").read_text(encoding="utf-8")
    )
    project = metadata["project"]
    assert project["name"] == "purra"
    assert project.get("dependencies") == []
    assert (ROOT_DIR / "src" / "purra" / "api" / "__init__.py").is_file()


def test_public_purra_readmes_do_not_bind_the_framework_to_a_product():
    forbidden = ("purrtypos", "screenplay", "writing-owned", "剧本", "写作领域")
    violations = [
        f"{path.name} contains {term!r}"
        for path in PACKAGE_READMES
        for term in forbidden
        if term in path.read_text(encoding="utf-8").casefold()
    ]
    assert not violations, "PurrA public docs contain product semantics:\n" + "\n".join(
        violations
    )


def test_portable_agent_conformance_uses_only_public_purra_boundaries():
    source = PORTABLE_CONFORMANCE.read_text(encoding="utf-8")
    imported_roots = {name.split(".", 1)[0] for name in _imports(PORTABLE_CONFORMANCE)}

    assert imported_roots <= {
        "__future__",
        "asyncio",
        "dataclasses",
        "datetime",
        "pytest",
        "purra",
    }
    assert "_execute_run" not in source
    assert "core.submit(" in source
    assert "backend" not in imported_roots


def test_host_adapter_conformance_has_no_test_framework_or_host_dependency():
    imported_roots = {
        name.split(".", 1)[0] for name in _imports(HOST_ADAPTER_CONFORMANCE)
    }

    assert imported_roots <= {
        "__future__",
        "asyncio",
        "collections",
        "dataclasses",
        "datetime",
        "json",
        "purra",
        "uuid",
    }
    assert "pytest" not in imported_roots
    assert "backend" not in imported_roots


def test_second_host_uses_public_composition_and_keeps_its_style_out_of_core():
    source = SECOND_HOST_CONFORMANCE.read_text(encoding="utf-8")
    imported_roots = {name.split(".", 1)[0] for name in _imports(SECOND_HOST_CONFORMANCE)}
    core_source = "\n".join(
        path.read_text(encoding="utf-8")
        for path in sorted((ROOT_DIR / "src" / "purra").rglob("*.py"))
    )

    assert imported_roots <= {
        "__future__",
        "asyncio",
        "dataclasses",
        "json",
        "pytest",
        "purra",
    }
    assert "core.submit(" in source
    assert "MessageOrigin.HOST_CONTEXT" in source
    assert "_execute_run" not in source
    assert "backend" not in imported_roots
    assert "operations.incident" not in core_source
    assert "readServiceStatus" not in core_source
