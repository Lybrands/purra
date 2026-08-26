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
export type AssistantContentWithToolCalls = "required" | "optional" | "forbidden";
export type InvocationOutputLimitSource = "user_override" | "model_profile" | "workflow_policy";

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
}

export interface ModelTokenUsage {
  readonly inputTokens: number;
  readonly outputTokens?: number;
  readonly totalTokens?: number;
  readonly cachedInputTokens?: number;
  readonly reasoningOutputTokens?: number;
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
  readonly toolCallDeltas?: readonly ToolCallDelta[];
  readonly finishReason?: ModelFinishReason;
  readonly usage?: ModelTokenUsage;
}

export type ModelStreamActivityKind = "transport" | "working";
export type ModelStreamActivitySupport = "semantic_only" | "transport" | "working";

export interface ModelStreamActivity {
  readonly type: "activity";
  readonly kind: ModelStreamActivityKind;
}

export type ModelStreamItem = ModelStreamChunk | ModelStreamActivity;

export interface ModelStream extends AsyncIterable<ModelStreamItem> {
  readonly activitySupport?: ModelStreamActivitySupport;
}

export interface ModelTurn {
  readonly message: Message;
  readonly finishReason: ModelFinishReason;
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
  readonly assistantContentWithToolCalls: AssistantContentWithToolCalls;
  readonly jsonSchemaLevel: string;
  readonly streamFinishSemantics: string;
  readonly usageSemantics: string;
}

export interface ModelCapabilitySnapshot {
  readonly schemaVersion: number;
  readonly profileId: string;
  readonly providerProtocol: string;
  readonly contextWindowTokens: number;
  readonly maxOutputTokens: number | null;
  readonly thinkingTokenAccounting: ThinkingTokenAccounting;
  readonly protocol: ModelProtocolCapabilities;
  readonly actionable?: boolean;
  readonly source?: string;
}

export interface InvocationOutputLimit {
  readonly maxTokens: number;
  readonly source: InvocationOutputLimitSource;
  readonly profileMaxTokens: number;
}

export interface ModelRequest {
  readonly messages: readonly Message[];
  readonly tools: readonly ToolSpec[];
  readonly capabilitySnapshot?: ModelCapabilitySnapshot;
  readonly outputLimit?: InvocationOutputLimit;
}

export interface ModelGateway {
  readonly capabilities?: ModelCapabilitySnapshot;
  invoke(request: ModelRequest, signal?: AbortSignal): Promise<ModelTurn>;
  stream?(
    request: ModelRequest,
    signal?: AbortSignal,
  ): ModelStream | Promise<ModelStream>;
}
