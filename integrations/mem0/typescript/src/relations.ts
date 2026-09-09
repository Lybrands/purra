import { MemoryError } from './journal.js';
import type { Mem0Memory, MemoryRef, MemorySource } from './memory.js';

export interface MemoryRelationProposal {
  readonly from: MemoryRef; readonly to: MemoryRef; readonly relation: string;
  readonly fromQuote: string; readonly toQuote: string;
  readonly fromSource: MemorySource; readonly toSource: MemorySource;
}
export interface MemoryRelationExtractionInput {
  readonly relations: readonly string[]; readonly maxProposals: number;
  readonly records: readonly { readonly id: string; readonly version: number; readonly text: string }[];
}
export type MemoryRelationExtractor = (input: MemoryRelationExtractionInput, signal?: AbortSignal) => Promise<unknown>;

/** Returns candidates only. The host owns model budgets, authorization and review. */
export async function proposeMemoryRelations(memory: Mem0Memory, refs: readonly MemoryRef[], options: {
  relations: readonly string[]; extract: MemoryRelationExtractor;
  maxInputChars?: number; maxProposals?: number; signal?: AbortSignal;
}): Promise<readonly MemoryRelationProposal[]> {
  const { extract, signal } = options;
  const maxInputChars = options.maxInputChars ?? 32_000, maxProposals = options.maxProposals ?? 32;
  if (typeof extract !== 'function') throw new TypeError('extract must be an async function');
  for (const [value, limit] of [[maxInputChars, 10_000_000], [maxProposals, 256]] as const) {
    if (!Number.isSafeInteger(value) || value < 1 || value > limit) throw new TypeError('Invalid relation extraction limit');
  }
  if (!Array.isArray(refs) || refs.length < 2 || refs.length > 32 || refs.some(ref =>
    !ref || typeof ref.id !== 'string' || !ref.id.trim() || ref.id.length > 512 ||
    !Number.isSafeInteger(ref.version) || ref.version < 1 || ref.version > 2 ** 31 - 2)) throw new TypeError('Provide 2 to 32 memory references');
  const selected = refs.map(ref => Object.freeze({ id: ref.id, version: ref.version }));
  if (new Set(selected.map(ref => ref.id)).size !== selected.length) throw new TypeError('Duplicate memory reference');
  if (!Array.isArray(options.relations) || options.relations.length < 1 || options.relations.length > 64 || options.relations.some(value =>
    typeof value !== 'string' || !value.trim() || value !== value.trim() || [...value].length > 64)) throw new TypeError('Invalid host relation vocabulary');
  const relations = Object.freeze([...options.relations]);
  if (new Set(relations).size !== relations.length) throw new TypeError('Duplicate relation');
  const epoch = memory.epoch;
  const check = () => {
    if (signal?.aborted) throw new MemoryError('memory_cancelled');
    memory.assertEpoch(epoch);
  };
  check();
  const records = new Map<string, NonNullable<Awaited<ReturnType<Mem0Memory['get']>>>>();
  for (const ref of selected) {
    const record = await memory.get(ref.id, signal === undefined ? {} : { signal });
    if (!record || record.version !== ref.version) throw new MemoryError('memory_relation_stale');
    records.set(ref.id, record);
  }
  if ([...records.values()].reduce((sum, record) => sum + [...record.text].length, 0) > maxInputChars) throw new TypeError('Relation source text exceeds maxInputChars');
  check();
  const raw = await extract(Object.freeze({ relations, maxProposals,
    records: Object.freeze([...records.values()].map(({ id, version, text }) => Object.freeze({ id, version, text }))),
  }), signal);
  check();
  if (!Array.isArray(raw) || raw.length > maxProposals) throw new MemoryError('memory_invalid_relation_proposal');
  const result: MemoryRelationProposal[] = [], seen = new Set<string>();
  for (const item of raw) {
    const keys = ['from', 'to', 'relation', 'fromQuote', 'toQuote'];
    if (!item || typeof item !== 'object' || Object.keys(item).length !== keys.length ||
      keys.some(key => !Object.hasOwn(item, key) || typeof item[key] !== 'string')) throw new MemoryError('memory_invalid_relation_proposal');
    const a = records.get(item.from), b = records.get(item.to);
    const identity = JSON.stringify([item.from, item.to, item.relation]);
    if (!a || !b || a.id === b.id || !relations.includes(item.relation) || seen.has(identity) ||
      !item.fromQuote.trim() || !a.text.includes(item.fromQuote) || !item.toQuote.trim() || !b.text.includes(item.toQuote)) throw new MemoryError('memory_invalid_relation_proposal');
    seen.add(identity);
    result.push(Object.freeze({ from: Object.freeze({ id: a.id, version: a.version }), to: Object.freeze({ id: b.id, version: b.version }),
      relation: item.relation, fromQuote: item.fromQuote, toQuote: item.toQuote,
      fromSource: Object.freeze({ ...a.source }), toSource: Object.freeze({ ...b.source }) }));
  }
  for (const ref of selected) {
    const before = records.get(ref.id)!, after = await memory.get(ref.id, signal === undefined ? {} : { signal });
    if (!after || after.version !== before.version || after.text !== before.text || after.state !== before.state ||
      after.source.id !== before.source.id || after.source.revision !== before.source.revision) throw new MemoryError('memory_relation_stale');
  }
  check();
  return Object.freeze(result);
}
