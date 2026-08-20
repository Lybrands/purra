"""Static dependency guard for the business-agnostic PurrA package."""

from __future__ import annotations

import ast
import importlib.util
import sys
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
CORE_SRC_DIR = ROOT_DIR / "src"
CORE_DIR = CORE_SRC_DIR / "purra"
ALLOWED_IMPORT_ROOTS = set(sys.stdlib_module_names) | {"__future__", "purra"}
FORBIDDEN_IMPORT_ROOTS = {"importlib", "sqlite3"}
BANNED_WRITING_IDENTIFIERS = {
    "bookId",
    "book_id",
    "chapterId",
    "chapter_id",
    "outlineId",
    "outline_id",
    "characterId",
    "character_id",
    "memoryId",
    "memory_id",
    "foreshadowingId",
    "foreshadowing_id",
    "settingEntityId",
    "setting_entity_id",
}
BANNED_WRITING_TEXT_FRAGMENTS = {
    "memory_context_receipts",
    "queryoutline",
    "getchaptercontent",
    "associatedoutlines",
    "associated chapters",
    "selected memories",
    "story_state",
    "大纲",
    "章节",
    "人物",
    "伏笔",
}
BANNED_SCREENPLAY_TEXT_FRAGMENTS = {
    "continuity_review",
    "durableexecutionplan",
    "episodes",
    "episode",
    "proposescenedraft",
    "scenes",
    "scene_generation",
    "screenplay",
    "taskadmissionvocabulary",
    "剧本",
}
BANNED_PROVIDER_TEXT_FRAGMENTS = {
    "deepseek",
    "kimi",
    "mimo",
    "zai",
}
PRODUCT_MODULE_PARTS = {"application", "screenplay", "writing"}


def _source_files() -> list[Path]:
    return sorted(CORE_DIR.rglob("*.py"))


def _module_for(path: Path) -> str:
    relative = path.relative_to(CORE_SRC_DIR).with_suffix("")
    return ".".join(relative.parts)


def _resolved_import(module_name: str, node: ast.ImportFrom) -> str:
    if not node.level:
        return node.module or ""
    package = module_name.rpartition(".")[0]
    return importlib.util.resolve_name("." * node.level + (node.module or ""), package)


def test_purra_imports_only_stdlib_and_itself():
    violations: list[str] = []
    for path in _source_files():
        module_name = _module_for(path)
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            imported: list[str] = []
            if isinstance(node, ast.Import):
                imported = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                imported = [_resolved_import(module_name, node)]
            for name in imported:
                root = name.split(".", 1)[0]
                if root in FORBIDDEN_IMPORT_ROOTS or root not in ALLOWED_IMPORT_ROOTS:
                    violations.append(
                        f"{path.relative_to(CORE_SRC_DIR)}:{node.lineno} imports {name}"
                    )
            if isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name) and node.func.id == "__import__":
                    violations.append(
                        f"{path.relative_to(CORE_SRC_DIR)}:{node.lineno} uses __import__"
                    )

    assert not violations, "PurrA dependency violations:\n" + "\n".join(violations)


def test_purra_durable_core_does_not_import_product_modules():
    path = CORE_DIR / "engine" / "durable_execution.py"
    module_name = _module_for(path)
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    imports = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imports.append(_resolved_import(module_name, node))

    leaked = sorted(
        name
        for name in imports
        if PRODUCT_MODULE_PARTS.intersection(name.casefold().split("."))
    )
    assert not leaked, "PurrA durable Core imports product modules: " + ", ".join(leaked)


def test_purra_does_not_name_writing_scope_fields():
    violations: list[str] = []
    for path in _source_files():
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            value: str | None = None
            if isinstance(node, ast.Name):
                value = node.id
            elif isinstance(node, ast.arg):
                value = node.arg
            elif isinstance(node, ast.Attribute):
                value = node.attr
            elif isinstance(node, ast.keyword):
                value = node.arg
            elif isinstance(node, ast.Constant) and isinstance(node.value, str):
                value = node.value if node.value in BANNED_WRITING_IDENTIFIERS else None
            if value in BANNED_WRITING_IDENTIFIERS:
                violations.append(
                    f"{path.relative_to(CORE_SRC_DIR)}:{getattr(node, 'lineno', 0)} names {value}"
                )

    assert not violations, "PurrA writing-scope leaks:\n" + "\n".join(violations)


def test_purra_prompts_do_not_embed_writing_domain_language():
    violations: list[str] = []
    for path in _source_files():
        source = path.read_text(encoding="utf-8").casefold()
        for fragment in sorted(BANNED_WRITING_TEXT_FRAGMENTS):
            if fragment.casefold() in source:
                violations.append(
                    f"{path.relative_to(CORE_SRC_DIR)} contains {fragment!r}"
                )

    assert not violations, "PurrA writing-text leaks:\n" + "\n".join(violations)


def test_purra_does_not_embed_screenplay_business_protocols():
    violations: list[str] = []
    for path in _source_files():
        source = path.read_text(encoding="utf-8").casefold()
        for fragment in sorted(BANNED_SCREENPLAY_TEXT_FRAGMENTS):
            if fragment in source:
                violations.append(
                    f"{path.relative_to(CORE_SRC_DIR)} contains {fragment!r}"
                )

    assert not violations, "PurrA screenplay leaks:\n" + "\n".join(violations)


def test_purra_does_not_embed_provider_identities():
    violations: list[str] = []
    for path in _source_files():
        source = path.read_text(encoding="utf-8").casefold()
        for fragment in sorted(BANNED_PROVIDER_TEXT_FRAGMENTS):
            if fragment in source:
                violations.append(
                    f"{path.relative_to(CORE_SRC_DIR)} contains {fragment!r}"
                )

    assert not violations, "PurrA provider leaks:\n" + "\n".join(violations)
