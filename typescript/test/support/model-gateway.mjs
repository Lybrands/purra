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
