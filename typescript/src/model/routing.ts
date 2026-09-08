import type { ModelCapabilitySnapshot } from './types.js';
import { copyCapabilitySnapshot } from './validation.js';
import { AgentError } from '../shared/errors.js';

export interface ModelRouteCandidate {
  readonly bindingId: string;
  readonly revision: string;
  readonly configIdentity: string;
  readonly capabilities: ModelCapabilitySnapshot;
}

export interface ModelRouteRequirements {
  readonly reasoningMode: 'default' | 'enabled' | 'disabled';
  readonly toolCalling?: 'required' | 'optional' | 'disabled';
  readonly structuredOutputLevel?: 'none' | 'unknown' | 'json_object' | 'json_schema';
  readonly streamingRequired?: boolean;
  readonly cancellationRequired?: boolean;
}

/** Pure registration-order selection. The result is not execution authority. */
export function selectModelRoute(candidates: readonly ModelRouteCandidate[], allowedBindingIds: readonly string[], requirements: ModelRouteRequirements): ModelRouteCandidate {
  const levels = { none: 0, unknown: 0, json_object: 1, json_schema: 2 };
  if (!['default', 'enabled', 'disabled'].includes(requirements.reasoningMode)
    || !['required', 'optional', 'disabled'].includes(requirements.toolCalling ?? 'optional')
    || !Object.hasOwn(levels, requirements.structuredOutputLevel ?? 'none')
    || [requirements.streamingRequired, requirements.cancellationRequired].some(v => v !== undefined && typeof v !== 'boolean')) throw new TypeError('Invalid model route requirements');
  const rows = candidates.map(row => {
    for (const value of [row.bindingId, row.revision, row.configIdentity]) {
      if (typeof value !== 'string' || !value.trim() || value !== value.trim()) throw new TypeError('Invalid model binding identity');
    }
    return { bindingId: row.bindingId, revision: row.revision, configIdentity: row.configIdentity, capabilities: copyCapabilitySnapshot(row.capabilities) };
  });
  const ids = new Set(rows.map(row => row.bindingId));
  if (ids.size !== rows.length || allowedBindingIds.some(id => !ids.has(id))) throw new TypeError('Duplicate candidate or unknown allowed binding');
  for (const row of rows) {
    const c = row.capabilities, p = c.protocol;
    if (!allowedBindingIds.includes(row.bindingId) || c.actionable === false || c.maxGenerationTokens === null
      || (requirements.reasoningMode === 'enabled' && p.reasoningControl === 'unavailable')
      || (requirements.reasoningMode === 'disabled' && p.reasoningControl === 'always_enabled')
      || (requirements.toolCalling === 'required' && p.toolCalling !== 'supported')
      || (requirements.streamingRequired && p.streaming !== 'supported')
      || (requirements.cancellationRequired && p.cancellation !== 'supported')
      || (levels[p.jsonSchemaLevel as keyof typeof levels] ?? 0) < levels[requirements.structuredOutputLevel ?? 'none']) continue;
    return Object.freeze(row);
  }
  throw new AgentError('model_route_unavailable', 'No authorized compatible model binding');
}
