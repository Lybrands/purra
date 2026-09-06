import type { JsonValue } from "../model/types.js";

export type JsonSchema = Readonly<Record<string, JsonValue>>;

const TYPES = new Set(["array", "boolean", "integer", "null", "number", "object", "string"]);
export const SCHEMA_KEYWORDS = new Set([
  "additionalProperties", "anyOf", "const", "description", "enum", "items",
  "maximum", "maxItems", "maxLength", "minimum", "minItems", "minLength",
  "oneOf", "properties", "required", "title", "type", "uniqueItems",
]);

export function inspectSchemaNode(schema: JsonValue, path: string, toolName: string): void {
  if (!isObject(schema)) throw invalidSchema(toolName, `${path} must be an object`);
  const unknown = Object.keys(schema).filter((key) => !SCHEMA_KEYWORDS.has(key));
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
      inspectSchemaNode(child, `${path}.properties.${name}`, toolName);
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
  if (schema.uniqueItems !== undefined && typeof schema.uniqueItems !== "boolean") {
    throw invalidSchema(toolName, `${path}.uniqueItems must be boolean`);
  }
  if (schema.items !== undefined) inspectSchemaNode(schema.items, `${path}.items`, toolName);

  for (const keyword of ["anyOf", "oneOf"] as const) {
    const branches = schema[keyword];
    if (branches === undefined) continue;
    if (!Array.isArray(branches) || branches.length === 0) {
      throw invalidSchema(toolName, `${path}.${keyword} must be a non-empty schema array`);
    }
    branches.forEach((branch, index) => inspectSchemaNode(branch, `${path}.${keyword}[${index}]`, toolName));
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

export interface SchemaViolation { readonly path: string; readonly keyword: string; readonly message: string; }

export function findSchemaViolation(value: JsonValue, schema: JsonSchema, path: string, unicode = false, work?: { remaining: number }): SchemaViolation | undefined {
  spend(work);
  const anyOf = schema.anyOf;
  if (Array.isArray(anyOf) && !anyOf.map((branch) => findSchemaViolation(value, branch as JsonSchema, path, unicode, work)).some((violation) => violation === undefined)) {
    return { path, keyword: "anyOf", message: `argument ${path} does not match any allowed schema variant` };
  }
  const oneOf = schema.oneOf;
  if (
    Array.isArray(oneOf)
    && oneOf.filter((branch) => findSchemaViolation(value, branch as JsonSchema, path, unicode, work) === undefined).length !== 1
  ) {
    return { path, keyword: "oneOf", message: `argument ${path} must match exactly one schema variant` };
  }

  if (schema.type !== undefined && !matchesType(value, schema.type)) {
    return { path, keyword: "type", message: `argument ${path} has type ${jsonType(value)}, expected ${typeLabel(schema.type)}` };
  }
  if (Array.isArray(schema.enum) && !schema.enum.some((candidate) => jsonEqual(value, candidate, work))) {
    return { path, keyword: "enum", message: `argument ${path} is not one of the allowed values` };
  }
  if (schema.const !== undefined && !jsonEqual(value, schema.const, work)) {
    return { path, keyword: "const", message: `argument ${path} does not match the required constant` };
  }

  if (typeof value === "string") {
    if (typeof schema.minLength === "number" && (unicode ? [...value].length : value.length) < schema.minLength) {
      return { path, keyword: "minLength", message: `argument ${path} is shorter than minLength ${schema.minLength}` };
    }
    if (typeof schema.maxLength === "number" && (unicode ? [...value].length : value.length) > schema.maxLength) {
      return { path, keyword: "maxLength", message: `argument ${path} exceeds maxLength ${schema.maxLength}` };
    }
  }
  if (Array.isArray(value)) {
    if (typeof schema.minItems === "number" && value.length < schema.minItems) {
      return { path, keyword: "minItems", message: `argument ${path} has fewer than minItems ${schema.minItems}` };
    }
    if (typeof schema.maxItems === "number" && value.length > schema.maxItems) {
      return { path, keyword: "maxItems", message: `argument ${path} exceeds maxItems ${schema.maxItems}` };
    }
    if (schema.uniqueItems === true) {
      for (let duplicateIndex = 1; duplicateIndex < value.length; duplicateIndex += 1) {
        for (let firstIndex = 0; firstIndex < duplicateIndex; firstIndex += 1) {
          if (jsonEqual(value[firstIndex]!, value[duplicateIndex]!, work)) {
            return { path, keyword: "uniqueItems", message: `argument ${path} violates uniqueItems at indexes ${firstIndex} and ${duplicateIndex}` };
          }
        }
      }
    }
    if (schema.items !== undefined) {
      for (let index = 0; index < value.length; index += 1) {
        const violation = findSchemaViolation(value[index]!, schema.items as JsonSchema, `${path}[${index}]`, unicode, work);
        if (violation !== undefined) return violation;
      }
    }
  }
  if (isObject(value)) {
    const properties = isObject(schema.properties) ? schema.properties : {};
    if (Array.isArray(schema.required)) {
      const missing = schema.required.find((name) => typeof name === "string" && !Object.hasOwn(value, name));
      if (typeof missing === "string") return { path: `${path}.${missing}`, keyword: "required", message: `argument ${path}.${missing} is required` };
    }
    if (schema.additionalProperties === false) {
      const unknown = Object.keys(value).filter((key) => !Object.hasOwn(properties, key));
      if (unknown.length > 0) {
        return { path, keyword: "additionalProperties", message: `argument ${path} contains undeclared properties: ${unknown.sort().join(", ")}` };
      }
    }
    for (const [key, item] of Object.entries(value)) {
      const child = Object.hasOwn(properties, key) ? properties[key] : undefined;
      if (child === undefined) continue;
      const violation = findSchemaViolation(item, child as JsonSchema, `${path}.${key}`, unicode, work);
      if (violation !== undefined) return violation;
    }
  }
  if (isJsonNumber(value)) {
    if (typeof schema.minimum === "number" && value < schema.minimum) {
      return { path, keyword: "minimum", message: `argument ${path} is below minimum ${schema.minimum}` };
    }
    if (typeof schema.maximum === "number" && value > schema.maximum) {
      return { path, keyword: "maximum", message: `argument ${path} exceeds maximum ${schema.maximum}` };
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

export function jsonEqual(left: JsonValue, right: JsonValue, work?: { remaining: number }): boolean {
  spend(work);
  if (left === right) return true;
  if (Array.isArray(left) && Array.isArray(right)) {
    return left.length === right.length && left.every((item, index) => jsonEqual(item, right[index]!, work));
  }
  if (isObject(left) && isObject(right)) {
    const leftKeys = Object.keys(left);
    const rightKeys = Object.keys(right);
    return leftKeys.length === rightKeys.length
      && leftKeys.every((key) => Object.hasOwn(right, key) && jsonEqual(left[key]!, right[key]!, work));
  }
  return false;
}

function invalidSchema(_label: string, message: string): TypeError {
  return new TypeError(message);
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

function spend(work?: { remaining: number }): void {
  if (work !== undefined && --work.remaining < 0) throw new Error("validation work limit exceeded");
}
