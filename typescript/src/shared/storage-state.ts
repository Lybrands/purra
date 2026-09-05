/** Data-only snapshots for version-pinned durable adapters. */
export function encodeStorageState(schema: string, value: unknown): string {
  function encode(item: unknown): unknown {
    if (item === undefined) return ["undefined"];
    if (item === null || typeof item === "string" || typeof item === "boolean") return ["value", item];
    if (typeof item === "number" && Number.isFinite(item)) return ["value", item];
    if (item instanceof Map) return ["map", [...item].map(([k, v]) => [encode(k), encode(v)])];
    if (item instanceof Set) return ["set", [...item].map(encode)];
    if (Array.isArray(item)) return ["array", item.map(encode)];
    if (typeof item === "object") return ["object", Object.entries(item).map(([k, v]) => [k, encode(v)])];
    throw new TypeError("Unsupported storage value");
  }
  return JSON.stringify({ schema, value: encode(value) });
}

export function decodeStorageState<T extends Record<string, unknown>>(text: string, schema: string, shape: T, optional: Record<string, unknown> = {}): T {
  function decode(row: any, depth = 0): unknown {
    if (depth > 128 || !Array.isArray(row) || row.length !== (row[0] === "undefined" ? 1 : 2)) throw new TypeError("Invalid storage encoding");
    if (["map", "set", "array", "object"].includes(row[0]) && !Array.isArray(row[1])) throw new TypeError("Invalid storage collection");
    switch (row[0]) {
      case "undefined": return undefined;
      case "value": {
        const v = row[1];
        if (v === null || typeof v === "string" || typeof v === "boolean" || (typeof v === "number" && Number.isFinite(v))) return v;
        throw new TypeError("Invalid storage scalar");
      }
      case "map": {
        const result = new Map();
        for (const pair of row[1]) {
          if (!Array.isArray(pair) || pair.length !== 2) throw new TypeError("Invalid storage pair");
          const key = decode(pair[0], depth + 1);
          if (result.has(key)) throw new TypeError("Duplicate storage key");
          result.set(key, decode(pair[1], depth + 1));
        }
        return result;
      }
      case "set": return new Set(row[1].map((v: unknown) => decode(v, depth + 1)));
      case "array": return row[1].map((v: unknown) => decode(v, depth + 1));
      case "object": {
        const result: Record<string, unknown> = {};
        for (const pair of row[1]) {
          if (!Array.isArray(pair) || pair.length !== 2 || typeof pair[0] !== "string" || Object.hasOwn(result, pair[0])) throw new TypeError("Invalid storage field");
          Object.defineProperty(result, pair[0], { value: decode(pair[1], depth + 1), enumerable: true, writable: true, configurable: true });
        }
        return result;
      }
      default: throw new TypeError("Unsupported storage encoding");
    }
  }
  const envelope = JSON.parse(text);
  if (envelope === null || typeof envelope !== "object" || envelope.schema !== schema || Object.keys(envelope).sort().join() !== "schema,value") throw new TypeError("Unsupported Core storage state");
  const value = decode(envelope.value);
  if (value === null || typeof value !== "object" || Array.isArray(value)) throw new TypeError("Invalid storage state");
  const saved = value as Record<string, unknown>;
  if (Object.keys(shape).some(key => !Object.hasOwn(saved, key)) || Object.keys(saved).some(key => !Object.hasOwn(shape, key) && !Object.hasOwn(optional, key))) throw new TypeError("Invalid storage fields");
  for (const key of Object.keys(saved)) {
    const expected = Object.hasOwn(shape, key) ? shape[key] : optional[key];
    if (expected instanceof Map ? !(saved[key] instanceof Map) : typeof saved[key] !== typeof expected) throw new TypeError("Invalid storage field type");
    if (typeof expected === "number" && (!Number.isSafeInteger(saved[key]) || (saved[key] as number) < 0)) throw new TypeError("Invalid storage counter");
  }
  return saved as T;
}

/** Reject unrecognized private record fields before installing a decoded state. */
export function requireStorageFields(value: unknown, required: readonly string[], optional: readonly string[] = []): void {
  if (value === null || typeof value !== "object" || Array.isArray(value)
    || required.some(key => !Object.hasOwn(value, key))
    || Object.keys(value).some(key => !required.includes(key) && !optional.includes(key))) {
    throw new TypeError("Invalid storage record fields");
  }
}
