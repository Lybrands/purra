import { AgentError } from "./shared/errors.js";

export class UserInputRequired extends AgentError {
  constructor(readonly runId: string, readonly requestId: string) {
    super("user_input_required", "The Run is waiting for user input");
  }
}
