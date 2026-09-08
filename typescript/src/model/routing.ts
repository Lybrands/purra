import type { ModelCapabilitySnapshot } from './types.js';
import { copyCapabilitySnapshot, copyJsonValue } from './validation.js';
import { stableFingerprint } from '../shared/fingerprint.js';
import { AgentError } from '../shared/errors.js';

function copyAllowedIds(value: readonly string[]): readonly string[] {
  if (!Array.isArray(value) || value.some(id => typeof id !== 'string' || !id.trim() || id !== id.trim())) throw new TypeError('Authorized bindings must be an array of nonempty canonical IDs');
  return Object.freeze([...value]);
}

export interface ModelRouteCandidate {
  readonly bindingId: string;
  readonly revision: string;
  readonly configIdentity: string;
  readonly capabilities: ModelCapabilitySnapshot;
  readonly policyId?: string;
  readonly policyRevision?: string;
}

export function copyModelRouteCandidate(row: ModelRouteCandidate): ModelRouteCandidate {
  for (const value of [row.bindingId, row.revision, row.configIdentity]) {
    if (typeof value !== 'string' || !value.trim() || value !== value.trim()) throw new TypeError('Invalid model binding identity');
  }
  if ((row.policyId === undefined) !== (row.policyRevision === undefined)) throw new TypeError('Incomplete route policy identity');
  for (const value of [row.policyId, row.policyRevision]) {
    if (value !== undefined && (typeof value !== 'string' || !value.trim() || value !== value.trim())) throw new TypeError('Invalid route policy identity');
  }
  return Object.freeze({bindingId:row.bindingId, revision:row.revision, configIdentity:row.configIdentity,
    capabilities:copyCapabilitySnapshot(row.capabilities),
    ...(row.policyId === undefined ? {} : {policyId:row.policyId, policyRevision:row.policyRevision!})});
}

export interface ModelRouteRequirements {
  readonly reasoningMode: 'default' | 'enabled' | 'disabled';
  readonly toolCalling?: 'required' | 'optional' | 'disabled';
  readonly structuredOutputLevel?: 'none' | 'unknown' | 'json_object' | 'json_schema';
  readonly streamingRequired?: boolean;
  readonly cancellationRequired?: boolean;
}

/** Pure registration-order selection. The result is not execution authority. */
export function selectModelRoute(candidates: readonly ModelRouteCandidate[], allowedBindingIds: readonly string[], requirements: ModelRouteRequirements, policy?: {readonly id: string; readonly revision: string}): ModelRouteCandidate {
  const allowed = copyAllowedIds(allowedBindingIds);
  if (policy !== undefined && [policy.id, policy.revision].some(value => typeof value !== 'string' || !value.trim() || value !== value.trim())) throw new TypeError('Invalid route policy identity');
  const levels = { none: 0, unknown: 0, json_object: 1, json_schema: 2 };
  if (!['default', 'enabled', 'disabled'].includes(requirements.reasoningMode)
    || !['required', 'optional', 'disabled'].includes(requirements.toolCalling ?? 'optional')
    || !Object.hasOwn(levels, requirements.structuredOutputLevel ?? 'none')
    || [requirements.streamingRequired, requirements.cancellationRequired].some(v => v !== undefined && typeof v !== 'boolean')) throw new TypeError('Invalid model route requirements');
  const rows = candidates.map(copyModelRouteCandidate);
  const ids = new Set(rows.map(row => row.bindingId));
  if (ids.size !== rows.length || allowed.some(id => !ids.has(id))) throw new TypeError('Duplicate candidate or unknown allowed binding');
  for (const row of rows) {
    const c = row.capabilities, p = c.protocol;
    if (!allowed.includes(row.bindingId) || c.actionable === false || c.maxGenerationTokens === null
      || (requirements.reasoningMode === 'enabled' && p.reasoningControl === 'unavailable')
      || (requirements.reasoningMode === 'disabled' && p.reasoningControl === 'always_enabled')
      || (requirements.toolCalling === 'required' && p.toolCalling !== 'supported')
      || (requirements.streamingRequired && p.streaming !== 'supported')
      || (requirements.cancellationRequired && p.cancellation !== 'supported')
      || (Object.hasOwn(levels, p.jsonSchemaLevel) ? levels[p.jsonSchemaLevel as keyof typeof levels] : 0) < levels[requirements.structuredOutputLevel ?? 'none']) continue;
    return policy === undefined ? row : copyModelRouteCandidate({...row, policyId:policy.id, policyRevision:policy.revision});
  }
  throw new AgentError('model_route_unavailable', 'No authorized compatible model binding');
}

/** Resolve saved canonical identity; never select again or authorize dispatch. */
export async function resolveModelRoute(candidates: readonly ModelRouteCandidate[], saved: ModelRouteCandidate, allowedBindingIds: readonly string[]): Promise<ModelRouteCandidate> {
  const allowed = copyAllowedIds(allowedBindingIds);
  const rows = candidates.map(copyModelRouteCandidate);
  const value = copyModelRouteCandidate(saved);
  if (new Set(rows.map(row => row.bindingId)).size !== rows.length) throw new TypeError('Duplicate model route candidate');
  const row = rows.find(row => row.bindingId === value.bindingId && allowed.includes(row.bindingId));
  if (row !== undefined) {
    const {policyId: _policyId, policyRevision: _policyRevision, ...binding} = row;
    const resolved = copyModelRouteCandidate({...binding,
      ...(value.policyId === undefined ? {} : {policyId:value.policyId, policyRevision:value.policyRevision!})});
    if (await stableFingerprint(copyJsonValue(resolved)) === await stableFingerprint(copyJsonValue(value))) return resolved;
  }
  throw new AgentError('model_route_mismatch', 'Saved model route is missing, revoked or changed');
}

export interface ModelRouteBinding<Host> {
  readonly candidate: ModelRouteCandidate;
  readonly create: (route: ModelRouteCandidate) => Promise<Host>;
}

/** Factories apply the route to the preset; callers own host disposal and public execution. */
export class ModelRouteRegistry<Host> {
  readonly #bindings: ReadonlyMap<string, ModelRouteBinding<Host>>;
  readonly #candidates: readonly ModelRouteCandidate[];

  constructor(bindings: readonly ModelRouteBinding<Host>[]) {
    const rows = bindings.map(binding => {
      if (typeof binding.create !== 'function') throw new TypeError('Model route binding requires a host factory');
      return Object.freeze({candidate:copyModelRouteCandidate(binding.candidate), create:binding.create});
    });
    if (new Set(rows.map(row => row.candidate.bindingId)).size !== rows.length) throw new TypeError('Duplicate model route binding');
    this.#bindings = new Map(rows.map(row => [row.candidate.bindingId, row]));
    this.#candidates = Object.freeze(rows.map(row => row.candidate));
  }

  async createNew(allowedBindingIds: readonly string[], requirements: ModelRouteRequirements,
    policy?: {readonly id: string; readonly revision: string}): Promise<Host> {
    const route = selectModelRoute(this.#candidates, allowedBindingIds, requirements, policy);
    return this.#bindings.get(route.bindingId)!.create(route);
  }

  async createRecovery(saved: ModelRouteCandidate, allowedBindingIds: readonly string[]): Promise<Host> {
    const route = await resolveModelRoute(this.#candidates, saved, allowedBindingIds);
    return this.#bindings.get(route.bindingId)!.create(route);
  }
}
