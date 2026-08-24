export class AgentError extends Error {
  public readonly code: string;

  public constructor(code: string, message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "AgentError";
    this.code = code;
  }
}

export class AgentCanceledError extends AgentError {
  public constructor(options?: ErrorOptions) {
    super("agent_canceled", "Agent run was canceled", options);
    this.name = "AgentCanceledError";
  }
}
