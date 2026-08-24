import type { JsonValue } from "../model/types.js";
import { copyJsonValue } from "../model/validation.js";

export async function stableFingerprint(value: JsonValue): Promise<string> {
  const canonical = JSON.stringify(sortJson(copyJsonValue(value)));
  const digest = await globalThis.crypto.subtle.digest(
    "SHA-256",
    new TextEncoder().encode(canonical),
  );
  return [...new Uint8Array(digest)]
    .map((byte) => byte.toString(16).padStart(2, "0"))
    .join("");
}

function sortJson(value: JsonValue): JsonValue {
  if (Array.isArray(value)) return value.map(sortJson);
  if (value !== null && typeof value === "object") {
    const mapping = value as Readonly<Record<string, JsonValue>>;
    return Object.fromEntries(
      Object.keys(mapping).sort().map((key) => [key, sortJson(mapping[key]!)]),
    );
  }
  return value;
}
