import type { JsonValue } from "../model/types.js";
import { copyJsonValue } from "../model/validation.js";
import { AgentError } from "../shared/errors.js";

export type JsonSchema = Readonly<Record<string, JsonValue>>;

const TYPES = new Set(["array", "boolean", "integer", "null", "number", "object", "string"]);
const KEYWORDS = new Set([
  "additionalProperties", "anyOf", "const", "description", "enum", "items",
  "maximum", "maxItems", "maxLength", "minimum", "minItems", "minLength",
  "oneOf", "properties", "required", "title", "type",
]);

export function inspectToolSchema(value: unknown, toolName: string): JsonSchema {
  let schema: JsonValue;
  try {
    schema = copyJsonValue(value);
  } catch (error) {
    throw invalidSchema(toolName, "schema must contain only finite JSON values", error);
  }
  if (!isObject(schema)) throw invalidSchema(toolName, "schema must be an object");
  inspectNode(schema, "$", toolName);
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
  const violation = findViolation(value, schema, "$");
  if (violation !== undefined) {
    throw new AgentError(
      "invalid_tool_arguments_schema",
      `Tool ${toolName} ${violation}`,
    );
  }
}

function inspectNode(schema: JsonValue, path: string, toolName: string): void {
  if (!isObject(schema)) throw invalidSchema(toolName, `${path} must be an object`);
  const unknown = Object.keys(schema).filter((key) => !KEYWORDS.has(key));
  if (unknown.length > 0) {
    throw invalidSchema(toolName, `${path} uses unsupported keyword(s): ${unknown.sort().join(", ")}`);
  }

  if (schema.type !== undefined) {
    const types = typeof schema.type === "string"
      ? [schema.type]
      : Array.isArray(schema.type) ? schema.type : [];
    if (types.length === 0 || types.some((item) => typeof item !== "string")) {
      throw invalidSchema(toolName, `${path}.type must be a string or non-empty string array`);
    }
    if (new Set(types).size !== types.length) {
      throw invalidSchema(toolName, `${path}.type values must be unique`);
    }
    const unsupported = types.filter((item) => !TYPES.has(item));
    if (unsupported.length > 0) {
      throw invalidSchema(toolName, `${path}.type uses unsupported type(s): ${unsupported.join(", ")}`);
    }
  }

  for (const keyword of ["title", "description"] as const) {
    if (schema[keyword] !== undefined && typeof schema[keyword] !== "string") {
      throw invalidSchema(toolName, `${path}.${keyword} must be a string`);
    }
  }

  if (schema.properties !== undefined) {
    if (!isObject(schema.properties)) {
      throw invalidSchema(toolName, `${path}.properties must be an object`);
    }
    for (const [name, child] of Object.entries(schema.properties)) {
      inspectNode(child, `${path}.properties.${name}`, toolName);
    }
  }

  if (schema.required !== undefined) {
    if (
      !Array.isArray(schema.required)
      || schema.required.some((name) => typeof name !== "string" || name.length === 0)
    ) {
      throw invalidSchema(toolName, `${path}.required must be an array of non-empty strings`);
    }
    if (new Set(schema.required).size !== schema.required.length) {
      throw invalidSchema(toolName, `${path}.required values must be unique`);
    }
  }

  if (
    schema.additionalProperties !== undefined
    && typeof schema.additionalProperties !== "boolean"
  ) {
    throw invalidSchema(toolName, `${path}.additionalProperties must be boolean`);
  }
  if (schema.items !== undefined) inspectNode(schema.items, `${path}.items`, toolName);

  for (const keyword of ["anyOf", "oneOf"] as const) {
    const branches = schema[keyword];
    if (branches === undefined) continue;
    if (!Array.isArray(branches) || branches.length === 0) {
      throw invalidSchema(toolName, `${path}.${keyword} must be a non-empty schema array`);
    }
    branches.forEach((branch, index) => inspectNode(branch, `${path}.${keyword}[${index}]`, toolName));
  }

  if (schema.enum !== undefined && (!Array.isArray(schema.enum) || schema.enum.length === 0)) {
    throw invalidSchema(toolName, `${path}.enum must be a non-empty array`);
  }
  inspectRange(schema, path, toolName, "minLength", "maxLength", isNonNegativeInteger);
  inspectRange(schema, path, toolName, "minItems", "maxItems", isNonNegativeInteger);
  inspectRange(schema, path, toolName, "minimum", "maximum", isJsonNumber);
}

function inspectRange(
  schema: JsonSchema,
  path: string,
  toolName: string,
  minimumName: "minLength" | "minItems" | "minimum",
  maximumName: "maxLength" | "maxItems" | "maximum",
  valid: (value: unknown) => boolean,
): void {
  const minimum = schema[minimumName];
  const maximum = schema[maximumName];
  if (minimum !== undefined && !valid(minimum)) {
    throw invalidSchema(toolName, `${path}.${minimumName} has an invalid value`);
  }
  if (maximum !== undefined && !valid(maximum)) {
    throw invalidSchema(toolName, `${path}.${maximumName} has an invalid value`);
  }
  if (typeof minimum === "number" && typeof maximum === "number" && minimum > maximum) {
    throw invalidSchema(toolName, `${path}.${minimumName} cannot exceed ${maximumName}`);
  }
}

function findViolation(value: JsonValue, schema: JsonSchema, path: string): string | undefined {
  const anyOf = schema.anyOf;
  if (Array.isArray(anyOf) && !anyOf.some((branch) => findViolation(value, branch as JsonSchema, path) === undefined)) {
    return `argument ${path} does not match any allowed schema variant`;
  }
  const oneOf = schema.oneOf;
  if (
    Array.isArray(oneOf)
    && oneOf.filter((branch) => findViolation(value, branch as JsonSchema, path) === undefined).length !== 1
  ) {
    return `argument ${path} must match exactly one schema variant`;
  }

  if (schema.type !== undefined && !matchesType(value, schema.type)) {
    return `argument ${path} has type ${jsonType(value)}, expected ${typeLabel(schema.type)}`;
  }
  if (Array.isArray(schema.enum) && !schema.enum.some((candidate) => jsonEqual(value, candidate))) {
    return `argument ${path} is not one of the allowed values`;
  }
  if (schema.const !== undefined && !jsonEqual(value, schema.const)) {
    return `argument ${path} does not match the required constant`;
  }

  if (typeof value === "string") {
    if (typeof schema.minLength === "number" && value.length < schema.minLength) {
      return `argument ${path} is shorter than minLength ${schema.minLength}`;
    }
    if (typeof schema.maxLength === "number" && value.length > schema.maxLength) {
      return `argument ${path} exceeds maxLength ${schema.maxLength}`;
    }
  }
  if (Array.isArray(value)) {
    if (typeof schema.minItems === "number" && value.length < schema.minItems) {
      return `argument ${path} has fewer than minItems ${schema.minItems}`;
    }
    if (typeof schema.maxItems === "number" && value.length > schema.maxItems) {
      return `argument ${path} exceeds maxItems ${schema.maxItems}`;
    }
    if (schema.items !== undefined) {
      for (let index = 0; index < value.length; index += 1) {
        const violation = findViolation(value[index]!, schema.items as JsonSchema, `${path}[${index}]`);
        if (violation !== undefined) return violation;
      }
    }
  }
  if (isObject(value)) {
    const properties = isObject(schema.properties) ? schema.properties : {};
    if (Array.isArray(schema.required)) {
      const missing = schema.required.find((name) => typeof name === "string" && !(name in value));
      if (typeof missing === "string") return `argument ${path}.${missing} is required`;
    }
    if (schema.additionalProperties === false) {
      const unknown = Object.keys(value).filter((key) => !(key in properties));
      if (unknown.length > 0) {
        return `argument ${path} contains undeclared properties: ${unknown.sort().join(", ")}`;
      }
    }
    for (const [key, item] of Object.entries(value)) {
      const child = properties[key];
      if (child === undefined) continue;
      const violation = findViolation(item, child as JsonSchema, `${path}.${key}`);
      if (violation !== undefined) return violation;
    }
  }
  if (isJsonNumber(value)) {
    if (typeof schema.minimum === "number" && value < schema.minimum) {
      return `argument ${path} is below minimum ${schema.minimum}`;
    }
    if (typeof schema.maximum === "number" && value > schema.maximum) {
      return `argument ${path} exceeds maximum ${schema.maximum}`;
    }
  }
  return undefined;
}

function matchesType(value: JsonValue, expected: JsonValue): boolean {
  if (Array.isArray(expected)) return expected.some((item) => matchesType(value, item));
  switch (expected) {
    case "object": return isObject(value);
    case "array": return Array.isArray(value);
    case "string": return typeof value === "string";
    case "integer": return typeof value === "number" && Number.isInteger(value);
    case "number": return isJsonNumber(value);
    case "boolean": return typeof value === "boolean";
    case "null": return value === null;
    default: return false;
  }
}

function typeLabel(value: JsonValue): string {
  return Array.isArray(value) ? value.join(" or ") : String(value);
}

function jsonType(value: JsonValue): string {
  if (value === null) return "null";
  if (Array.isArray(value)) return "array";
  if (isObject(value)) return "object";
  if (typeof value === "number" && Number.isInteger(value)) return "integer";
  return typeof value;
}

function jsonEqual(left: JsonValue, right: JsonValue): boolean {
  if (left === right) return true;
  if (Array.isArray(left) && Array.isArray(right)) {
    return left.length === right.length && left.every((item, index) => jsonEqual(item, right[index]!));
  }
  if (isObject(left) && isObject(right)) {
    const leftKeys = Object.keys(left);
    const rightKeys = Object.keys(right);
    return leftKeys.length === rightKeys.length
      && leftKeys.every((key) => key in right && jsonEqual(left[key]!, right[key]!));
  }
  return false;
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

function isNonNegativeInteger(value: unknown): boolean {
  return Number.isSafeInteger(value) && Number(value) >= 0;
}

function isJsonNumber(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value);
}
