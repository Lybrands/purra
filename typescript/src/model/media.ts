import type { JsonValue } from "./types.js";
import type { FeatureSupport, Message } from "./types.js";
import { AgentError } from "../shared/errors.js";

export function assertImageInputSupport(messages: readonly Message[], support: FeatureSupport | undefined): void {
  if (messages.some(message => parseStaticImageContent(message.content) !== undefined) && support !== "supported") {
    throw new AgentError("model_capability_incompatible", "Model profile does not declare image input support");
  }
}

export const STATIC_IMAGE_PROFILE = "purra.static-images/v1";
export type StaticImage = {
  readonly mediaType: "image/png" | "image/jpeg" | "image/webp";
  readonly dataBase64: string;
  /** Host-supplied image token allowance for the selected model. */
  readonly inputTokens: number;
};
export type StaticImageContent = {
  readonly type: typeof STATIC_IMAGE_PROFILE;
  readonly text: string;
  readonly images: readonly StaticImage[];
};

export function staticImageContent(text: string, images: readonly StaticImage[]): StaticImageContent {
  return parseStaticImageContent({ type: STATIC_IMAGE_PROFILE, text, images })!;
}

export function parseStaticImageContent(value: JsonValue): StaticImageContent | undefined {
  if (value === null || typeof value !== "object" || Array.isArray(value)
    || !("type" in value) || value.type !== STATIC_IMAGE_PROFILE) return undefined;
  const row = value as Record<string, JsonValue>;
  if (Object.keys(row).sort().join(",") !== "images,text,type" || typeof row.text !== "string"
    || !Array.isArray(row.images) || !row.images.length) throw new TypeError("Invalid static image content");
  let total = 0;
  const images = row.images.map((item: JsonValue) => {
    if (item === null || typeof item !== "object" || Array.isArray(item)
      || Object.keys(item).sort().join(",") !== "dataBase64,inputTokens,mediaType") throw new TypeError("Invalid static image fields");
    const image = item as Record<string, JsonValue>;
    if (!["image/png", "image/jpeg", "image/webp"].includes(image.mediaType as string)) throw new TypeError("Unsupported static image media type");
    const data = image.dataBase64;
    if (typeof data !== "string" || !data) throw new TypeError("Static image data must be canonical base64");
    try {
      if (btoa(atob(data)) !== data) throw new Error();
    } catch {
      throw new TypeError("Static image data must be canonical base64");
    }
    const tokens = image.inputTokens;
    if (typeof tokens !== "number" || !Number.isSafeInteger(tokens) || tokens <= 0) throw new TypeError("Static image inputTokens must be a positive safe integer");
    total += tokens;
    if (!Number.isSafeInteger(total)) throw new TypeError("Static image token allowance exceeds safe integer range");
    return Object.freeze({ mediaType: image.mediaType as StaticImage["mediaType"], dataBase64: data, inputTokens: tokens });
  });
  return Object.freeze({ type: STATIC_IMAGE_PROFILE, text: row.text, images: Object.freeze(images) });
}

export function imageInputTokens(content: JsonValue): number {
  return parseStaticImageContent(content)?.images.reduce((sum, image) => sum + image.inputTokens, 0) ?? 0;
}
