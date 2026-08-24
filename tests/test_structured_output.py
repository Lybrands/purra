from __future__ import annotations

import pytest

from purra.structured_output import (
    StructuredOutputParseError,
    extract_json_object_tolerant,
    parse_json_object,
)


@pytest.mark.parametrize(
    ("content", "expected"),
    [
        ('{"ok":true}', {"ok": True}),
        ('```json\n{"ok":true}\n```', {"ok": True}),
        ('```\n{"ok":true}\n```', {"ok": True}),
    ],
)
def test_strict_json_object_accepts_only_a_complete_document(content, expected):
    assert parse_json_object(content) == expected


@pytest.mark.parametrize(
    "content",
    [
        'prefix {"ok":true}',
        '{"ok":true} suffix',
        '{"ok":true}.',
        '{"ok":true}{"other":false}',
        '{"ok":',
        '[{"ok":true}]',
        '```json\n{"ok":true}\n``` trailing',
    ],
)
def test_strict_json_object_rejects_non_authoritative_wrapping(content):
    with pytest.raises(StructuredOutputParseError):
        parse_json_object(content)


def test_tolerant_extraction_is_explicitly_separate():
    assert extract_json_object_tolerant('prefix {"ok":true} suffix') == {
        "ok": True,
    }
