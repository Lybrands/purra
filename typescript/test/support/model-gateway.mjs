const DEFAULT_CAPABILITIES = Object.freeze({
  schemaVersion: 2,
  profileId: "test-default",
  providerProtocol: "custom",
  contextWindowTokens: 1_000_000,
  maxGenerationTokens: 65_536,
  thinkingTokenAccounting: "unknown",
  protocol: Object.freeze({
    reasoningControl: "selectable",
    reasoningReplay: "ignored",
    toolCalling: "supported",
    requiredToolChoice: "supported",
    parallelToolCalls: "supported",
    streaming: "unknown",
    cancellation: "supported",
    publicProgress: "supported",
    assistantContentWithToolCalls: "optional",
    jsonSchemaLevel: "unknown",
    streamFinishSemantics: "normalized",
    usageSemantics: "normalized",
  }),
});

export function testGateway(delegate) {
  const capabilities = delegate.capabilities ?? DEFAULT_CAPABILITIES;
  return Object.freeze({
    ...delegate,
    capabilities,
    async invoke(request, signal) {
      const turn = await delegate.invoke(request, signal);
      return Object.freeze({
        ...turn,
        appliedGenerationLimit: request.outputBudget.maxGenerationTokens,
      });
    },
    ...(typeof delegate.stream !== "function" ? {} : {
      async stream(request, signal) {
        const stream = await delegate.stream(request, signal);
        return Object.freeze({
          appliedGenerationLimit: request.outputBudget.maxGenerationTokens,
          ...(stream.activitySupport === undefined ? {} : { activitySupport: stream.activitySupport }),
          ...(stream.transportDiagnostics === undefined ? {} : { transportDiagnostics: stream.transportDiagnostics }),
          [Symbol.asyncIterator]() { return stream[Symbol.asyncIterator](); },
        });
      },
    }),
  });
}


// Deterministic fixture adapter. Production presentation never buffers a turn.
export function treeTestGateway(delegate) {
  const base = delegate.capabilities ?? DEFAULT_CAPABILITIES;
  return testGateway({
    ...delegate,
    capabilities: {...base, protocol: {...base.protocol, streaming: "supported"}},
    async *stream(request, signal) {
      if (request.messages.some(m => typeof m.content === "string" && m.content.includes("Provide a concise progress update"))) {
        yield {contentDelta: "Available child ", appliedGenerationLimit: request.outputBudget.maxGenerationTokens};
        yield {contentDelta: "result received.", finishReason: "stop"};
        return;
      }
      const turn = await delegate.invoke(request, signal);
      yield {
        contentDelta: turn.message.content,
        ...(turn.message.toolCalls === undefined ? {} : {toolCallDeltas: turn.message.toolCalls.map((c, index) => ({index, id:c.id, name:c.name, argumentsFragment:JSON.stringify(c.arguments)}))}),
        finishReason: turn.finishReason,
        ...(turn.usage === undefined ? {} : {usage:turn.usage}),
      };
    },
  });
}
