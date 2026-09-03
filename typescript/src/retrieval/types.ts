import type { JsonValue } from "../model/types.js";

export interface RetrievalRequest {
  readonly query: string;
  readonly limit: number;
  readonly runId?: string;
  readonly scope: Readonly<Record<string, JsonValue>>;
}

export interface RetrievalHit {
  readonly id: string;
  readonly content: string;
  readonly source: string;
  readonly version?: number;
  readonly score?: number;
  readonly untrusted: boolean;
  readonly metadata: Readonly<Record<string, JsonValue>>;
}

export interface Retriever {
  retrieve(
    request: RetrievalRequest,
    signal?: AbortSignal,
  ): Promise<readonly RetrievalHit[]>;
}
