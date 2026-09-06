import { StructuredOutputError } from "purra";
import type { ModelCapabilitySnapshot, ModelRequest, JsonValue } from "purra";
const allowed = new Set(["type", "properties", "required", "additionalProperties", "items", "enum", "description", "title"]);
export function validateOutput(request: ModelRequest, capabilities: ModelCapabilitySnapshot): string | undefined {
  const output = request.outputContract;
  if (output === undefined || output.mode === "local") return undefined;
  function reject(): never { throw new StructuredOutputError("structured_output_mode_unsupported", "nativeDialect"); }
  if (request.tools.length || capabilities.protocol.jsonSchemaLevel !== "json_schema"
    || request.capabilitySnapshot?.protocol.jsonSchemaLevel !== "json_schema") reject();
  let enumCount = 0, propertyCount = 0;
  function visit(node: Readonly<Record<string, JsonValue>>, depth: number): void {
    if (depth > 8 || Object.keys(node).some(key => !allowed.has(key))) reject();
    let kind = node.type;
    if (typeof kind !== "string") {
      if (!Array.isArray(kind) || kind.length !== 2 || !kind.includes("null")) reject();
      kind = kind.find(t => t !== "null")!;
    }
    if (kind === "object") {
      const props = (node.properties ?? {}) as Readonly<Record<string, Readonly<Record<string, JsonValue>>>>;
      propertyCount += Object.keys(props).length;
      const required = (node.required ?? []) as readonly JsonValue[];
      if (propertyCount > 100 || node.additionalProperties !== false || !Array.isArray(required)
        || required.length !== Object.keys(props).length
        || Object.keys(props).some(key => !required.includes(key))) reject();
      for (const child of Object.values(props)) visit(child, depth + 1);
    } else if (["properties", "required", "additionalProperties"].some(key => key in node)) reject();
    if (kind === "array") {
      if (!node.items || typeof node.items !== "object" || Array.isArray(node.items)) reject();
      visit(node.items as Readonly<Record<string, JsonValue>>, depth + 1);
    } else if ("items" in node) reject();
    if (Array.isArray(node.enum)) {
      enumCount += node.enum.length;
      if (enumCount > 100 || node.enum.some(v => v !== null && typeof v === "object")) reject();
    }
  }
  visit(output.schema, 0);
  return "purra.anthropic-json-schema/v1";
}
