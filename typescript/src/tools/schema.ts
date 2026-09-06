import type { JsonValue } from "../model/types.js";
import { copyJsonValue } from "../model/validation.js";
import { AgentError } from "../shared/errors.js";
import { inspectSchemaNode, findSchemaViolation, type JsonSchema } from "../shared/schema.js";
export type { JsonSchema } from "../shared/schema.js";

export function inspectToolSchema(value: unknown, toolName: string): JsonSchema {
  let schema: JsonValue;
  try {
    schema = copyJsonValue(value);
  } catch (error) {
    throw invalidSchema(toolName, "schema must contain only finite JSON values", error);
  }
  if (!isObject(schema)) throw invalidSchema(toolName, "schema must be an object");
  try {
    inspectSchemaNode(schema, "$", toolName);
  } catch (error) {
    throw invalidSchema(toolName, error instanceof Error ? error.message : "invalid schema");
  }
  if (schema.type !== "object") {
    throw invalidSchema(toolName, "root schema type must be object");
  }
  return schema;
}

export function validateToolArguments(
  value: JsonValue,
  schema: JsonSchema,
  toolName: string,
): void {
  const violation = findSchemaViolation(value, schema, "$");
  if (violation !== undefined) {
    throw new AgentError(
      "invalid_tool_arguments_schema",
      `Tool ${toolName} ${violation.message}`,
    );
  }
}

function invalidSchema(toolName: string, message: string, cause?: unknown): AgentError {
  return new AgentError(
    "invalid_tool_schema",
    `Tool ${toolName} ${message}`,
    cause === undefined ? undefined : { cause },
  );
}

function isObject(value: unknown): value is Readonly<Record<string, JsonValue>> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}
