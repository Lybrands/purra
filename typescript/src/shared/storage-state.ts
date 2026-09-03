/** Data-only snapshots for version-pinned durable adapters. */
export function encodeStorageState(value: unknown): string {
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
  return JSON.stringify(encode(value));
}

export function decodeStorageState(text: string): unknown {
  function decode(row: any): unknown {
    switch (row[0]) {
      case "undefined": return undefined;
      case "value": return row[1];
      case "map": return new Map(row[1].map(([k, v]: any[]) => [decode(k), decode(v)]));
      case "set": return new Set(row[1].map(decode));
      case "array": return row[1].map(decode);
      case "object": return Object.fromEntries(row[1].map(([k, v]: any[]) => [k, decode(v)]));
      default: throw new TypeError("Unsupported storage encoding");
    }
  }
  return decode(JSON.parse(text));
}
