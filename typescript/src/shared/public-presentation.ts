import type { Message } from "../model/types.js";

export const FINAL_PUBLIC_PRESENTATION_GUIDANCE = [
  "The preceding assistant content came from a private tool-capable model round and was not shown to the user.",
  "Return the final user-facing answer now in this tool-free response.",
  "Preserve supported facts, do not mention this handoff, and do not imitate or request a tool call.",
].join(" ");

export function publicPresentationMessages(candidate: Message): readonly Message[] {
  return Object.freeze([
    Object.freeze({
      ...candidate,
      attributes: Object.freeze({
        ...(candidate.attributes ?? {}),
        publicPresentationCandidate: true,
      }),
    }),
    Object.freeze({
      role: "developer" as const,
      content: FINAL_PUBLIC_PRESENTATION_GUIDANCE,
      attributes: Object.freeze({ publicPresentation: true }),
    }),
  ]);
}

export function isPrivatePresentationMessage(message: Message): boolean {
  return message.attributes?.publicPresentationCandidate === true
    || message.attributes?.publicPresentation === true;
}

