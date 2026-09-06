export type JsonValue =
  | null
  | boolean
  | number
  | string
  | readonly JsonValue[]
  | { readonly [key: string]: JsonValue };

export type MessageRole = "system" | "developer" | "user" | "assistant" | "tool";
export type ModelFinishReason = "stop" | "tool_calls" | "length" | "filtered" | "other";
export type FeatureSupport = "supported" | "unavailable" | "unknown";
export type ReasoningControl = "selectable" | "always_enabled" | "unavailable";
export type ReasoningReplayPolicy = "required" | "forbidden" | "ignored";
export type ThinkingTokenAccounting = "included" | "separate" | "unknown";
export type ReasoningUsageDetail = "required" | "optional" | "unavailable";
export type ReasoningLimitKind = "none" | "soft" | "hard";
export type VisibleOutputReservation = "supported" | "unavailable" | "unknown";
export type LengthReasonDetail = "request_cap" | "context_cap" | "conflated";
export type ContinuationKind = "none" | "prefix_beta" | "opaque_state" | "signed_replay";
export type ContinuationSafety = "text" | "structured" | "tool_call";
export type AssistantContentWithToolCalls = "required" | "optional" | "forbidden";
export type GenerationBudgetSource = "user" | "model_profile" | "context_capacity";
export type ResultCapacitySource = "user" | "workflow_policy";

export interface ToolCall {
  readonly id: string;
  readonly name: string;
  readonly arguments: JsonValue;
}

export interface Message {
  readonly role: MessageRole;
  readonly content: JsonValue;
  readonly reasoning?: string;
  readonly toolCallId?: string;
  readonly toolCalls?: readonly ToolCall[];
  readonly attributes?: Readonly<Record<string, JsonValue>>;
  /** Private provider continuation state; omitted from public message projections. */
  readonly providerData?: Readonly<Record<string, JsonValue>>;
}

export interface ModelTokenUsage {
  readonly inputTokens: number;
  readonly generationTokens: number;
  readonly totalTokens?: number;
  readonly cachedInputTokens?: number;
  readonly reasoningTokens?: number;
}

export interface ToolCallDelta {
  readonly index: number;
  readonly id?: string;
  readonly type?: string;
  readonly name?: string;
  readonly argumentsFragment?: string;
}

export interface ModelStreamChunk {
  readonly contentDelta?: string;
  readonly reasoningDelta?: string;
  readonly progressDelta?: string;
  readonly toolCallDeltas?: readonly ToolCallDelta[];
  readonly finishReason?: ModelFinishReason;
  readonly usage?: ModelTokenUsage;
  /** Opaque provider continuation data, accepted only on the terminal chunk. */
  readonly providerData?: Readonly<Record<string, JsonValue>>;
}

export type ModelStreamActivityKind = "transport" | "working";
export type ModelStreamActivitySupport = "semantic_only" | "transport" | "working";

export interface ModelTransportDiagnostics {
  readonly requestSentAtMs?: number;
  readonly firstByteAtMs?: number;
  readonly httpAttempts?: number;
}

export interface ModelStreamActivity {
  readonly transportDiagnostics?: ModelTransportDiagnostics;
  readonly type: "activity";
  readonly kind: ModelStreamActivityKind;
}

export type ModelStreamItem = ModelStreamChunk | ModelStreamActivity;

export interface ModelStream extends AsyncIterable<ModelStreamItem> {
  readonly transportDiagnostics?: ModelTransportDiagnostics;
  /** Exact total-generation limit actually applied by the Provider host. */
  readonly appliedGenerationLimit: number;
  readonly activitySupport?: ModelStreamActivitySupport;
}

export interface ModelTurn {
  readonly message: Message;
  readonly finishReason: ModelFinishReason;
  /** Exact total-generation limit actually applied by the Provider host. */
  readonly appliedGenerationLimit: number;
  readonly usage?: ModelTokenUsage;
}

export interface ToolSpec {
  readonly name: string;
  readonly description: string;
  readonly displayNames?: Readonly<Record<string, string>>;
  readonly inputSchema: Readonly<Record<string, JsonValue>>;
}

export interface ModelProtocolCapabilities {
  readonly reasoningControl: ReasoningControl;
  readonly reasoningReplay: ReasoningReplayPolicy;
  readonly toolCalling: FeatureSupport;
  readonly requiredToolChoice: FeatureSupport;
  readonly parallelToolCalls: FeatureSupport;
  readonly streaming: FeatureSupport;
  readonly cancellation: FeatureSupport;
  readonly publicProgress?: FeatureSupport;
  readonly assistantContentWithToolCalls: AssistantContentWithToolCalls;
  readonly jsonSchemaLevel: string;
  readonly streamFinishSemantics: string;
  readonly usageSemantics: string;
}

export interface ModelCapabilitySnapshot {
  readonly schemaVersion: 2;
  readonly profileId: string;
  readonly providerProtocol: string;
  readonly contextWindowTokens: number;
  readonly maxGenerationTokens: number | null;
  readonly thinkingTokenAccounting: ThinkingTokenAccounting;
  readonly reasoningUsageDetail?: ReasoningUsageDetail;
  readonly reasoningLimitKind?: ReasoningLimitKind;
  readonly visibleOutputReservation?: VisibleOutputReservation;
  readonly lengthReasonDetail?: LengthReasonDetail;
  readonly continuationKind?: ContinuationKind;
  readonly continuationSafeFor?: readonly ContinuationSafety[];
  readonly protocol: ModelProtocolCapabilities;
  readonly actionable?: boolean;
  readonly source?: string;
}

export interface InvocationOutputBudget {
  /** Provider allowance for all generated tokens, including reasoning. */
  readonly maxGenerationTokens: number;
  readonly generationSource: GenerationBudgetSource;
  readonly profileMaxGenerationTokens: number;
  /** Original user ceiling, preserved when context capacity clips the effective value. */
  readonly requestedUserMaxGenerationTokens: number | null;
  /** Sizing/diagnostic target, not a Provider guarantee or actual minimum. */
  readonly resultCapacityTargetTokens: number | null;
  readonly resultCapacitySource: ResultCapacitySource | null;
  readonly nonResultHeadroomTokens: number | null;
}

export interface ModelRequest {
  readonly outputContract?: import("../structured.js").StructuredOutputContract;
  readonly messages: readonly Message[];
  readonly tools: readonly ToolSpec[];
  readonly capabilitySnapshot: ModelCapabilitySnapshot;
  readonly outputBudget: InvocationOutputBudget;
}

export interface ModelGateway {
  /** Pure preflight; returns the adapter's versioned native dialect, never performs I/O. */
  validateOutputContract?(request: ModelRequest): string | undefined;
  readonly capabilities: ModelCapabilitySnapshot;
  invoke(request: ModelRequest, signal?: AbortSignal): Promise<ModelTurn>;
  stream?(
    request: ModelRequest,
    signal?: AbortSignal,
  ): ModelStream | Promise<ModelStream>;
}
