"""Pure admission for the adapter's versioned native output dialect."""
from collections.abc import Mapping, Sequence
from purra.structured import StructuredOutputError

DIALECT = "purra.anthropic-json-schema/v1"
_ALLOWED = {"type", "properties", "required", "additionalProperties", "items", "enum", "description", "title"}

def validate(invocation):
    output = invocation.output_contract
    if output is None or output.mode == "local":
        return None
    def reject():
        raise StructuredOutputError("Native output dialect is unsupported", code="structured_output_mode_unsupported")
    if invocation.tools or invocation.request.protocol_capabilities.json_schema_level != "json_schema":
        reject()
    enum_count = 0
    property_count = 0
    def visit(node, depth):
        nonlocal enum_count, property_count
        if depth > 8 or set(node) - _ALLOWED:
            reject()
        kind = node.get("type")
        if not isinstance(kind, str):
            if not isinstance(kind, Sequence) or len(kind) != 2 or "null" not in kind:
                reject()
            kind = next(t for t in kind if t != "null")
        if kind == "object":
            props = node.get("properties", {})
            property_count += len(props)
            if property_count > 100 or node.get("additionalProperties") is not False or set(node.get("required", ())) != set(props):
                reject()
            for child in props.values():
                visit(child, depth + 1)
        elif "properties" in node or "required" in node or "additionalProperties" in node:
            reject()
        if kind == "array":
            if not isinstance(node.get("items"), Mapping):
                reject()
            visit(node["items"], depth + 1)
        elif "items" in node:
            reject()
        if "enum" in node:
            enum_count += len(node["enum"])
            if enum_count > 100 or any(isinstance(v, (Mapping, Sequence)) and not isinstance(v, str) for v in node["enum"]):
                reject()
    visit(output.schema, 0)
    return DIALECT
