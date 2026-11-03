"""JSON escaping must not erase tool-delivered documents or weaken redaction."""
import json

import pytest

from purra.tools.security import sanitize_tool_result, sanitize_error_message, summarize_tool_arguments


@pytest.mark.parametrize('ensure_ascii', [True, False])
def test_json_document_text_survives_newlines_and_relative_resource_links(ensure_ascii):
    document = ('---\nname: novel-method\nmetadata:\n  autoUse: true\n---\n'
                '# 来源\nEvent:\n陈沐周三发现刻字；形成时间未知。\n'
                '[资料](references/materials.md)\n[格式](ofm-syntax.md)\n')
    text = json.dumps({'file': {'content':document}, 'items':[document]}, ensure_ascii=ensure_ascii, indent=2)
    assert sanitize_tool_result(text) == text
    assert json.loads(sanitize_tool_result(text))['file']['content'] == document
    assert sanitize_error_message(json.dumps({'detail':'Event:\nnot a drive'})) == json.dumps({'detail':'Event:\nnot a drive'})
    assert json.loads(summarize_tool_arguments({'text':'Event:\nnot a drive'})) == {'text':'Event:\nnot a drive'}


@pytest.mark.parametrize('path', [r'C:\Users\Author\private.txt', '/Users/author/private/file.txt'])
def test_real_paths_and_credentials_inside_json_are_redacted_without_invalidating_json(path):
    payload = {'path':path, 'nested':[{'secret':'Bearer synthetic_token_123', 'key':'sk-syntheticsecret123'}],
               'text':'metadata:\n保留正文', path:'sensitive key'}
    sanitized = sanitize_tool_result(json.dumps(payload, ensure_ascii=False))
    result = json.loads(sanitized)
    assert path not in sanitized
    assert result['path'] == '[REDACTED_PATH]'
    assert result['nested'] == [{'secret':'Bearer [REDACTED]', 'key':'[REDACTED_KEY]'}]
    assert result['text'] == payload['text']
    assert result['[REDACTED_PATH]'] == 'sensitive key'


def test_plain_text_redaction_and_result_size_limit_still_apply():
    assert sanitize_tool_result(r'error at C:\Users\Author\private.txt') == 'error at [REDACTED_PATH]'
    assert '[REDACTED_PATH]' in sanitize_tool_result('error at /Users/author/private/file.txt')
    assert 'tool_result_too_large' in sanitize_tool_result('x'*20, max_chars=10)
