"""Resolve host-owned localized tool labels without changing protocol names."""

from __future__ import annotations

from dataclasses import replace
from typing import Iterable

from purra.contracts import ToolSchema, normalize_locale_tag


def resolve_tool_display_name(
    schema: ToolSchema,
    locale: str | None,
    *,
    fallback_locales: Iterable[str] = ("zh-CN", "en-US"),
) -> str:
    """Resolve exact, language-only, configured fallback, then protocol name."""

    names = schema.display_names
    candidates: list[str] = []
    if str(locale or "").strip():
        normalized = _safe_locale_tag(locale)
        if normalized:
            candidates.extend((normalized, normalized.split("-", 1)[0]))
    for fallback in fallback_locales:
        normalized = _safe_locale_tag(fallback)
        if not normalized:
            continue
        candidates.extend((normalized, normalized.split("-", 1)[0]))
    for candidate in dict.fromkeys(candidates):
        if candidate in names:
            return str(names[candidate])
        language_match = next(
            (
                str(display_name)
                for tag, display_name in names.items()
                if str(tag).split("-", 1)[0] == candidate
            ),
            None,
        )
        if language_match:
            return language_match
    return str(next(iter(names.values()), schema.name))


def model_visible_tool_schema(
    schema: ToolSchema,
    locale: str | None,
) -> ToolSchema:
    """Add one localized narration hint while preserving the call identifier."""

    display_name = resolve_tool_display_name(schema, locale)
    if display_name == schema.name:
        return schema
    language = (_safe_locale_tag(locale) or "zh-CN").split("-", 1)[0]
    if language == "zh":
        guidance = (
            f"面向用户描述本工具时，请使用展示名称“{display_name}”，"
            f"不要展示内部协议名称“{schema.name}”。"
        )
    else:
        guidance = (
            f'When referring to this tool in user-facing text, use "{display_name}" '
            f'instead of the internal protocol name "{schema.name}".'
        )
    return replace(
        schema,
        description=f"{schema.description.rstrip()}\n{guidance}",
    )


def _safe_locale_tag(value: object) -> str | None:
    try:
        return normalize_locale_tag(value)
    except ValueError:
        return None


__all__ = ["model_visible_tool_schema", "resolve_tool_display_name"]
