"""Core-owned state bridge for transaction-scoped durable adapters.

This is a versioned adapter contract, not the layout of the in-memory stores.
One session belongs to one storage transaction and must not escape it.
"""
from dataclasses import dataclass
from purra.adapters import InMemoryAgentAdapters
from purra.contracts import RunCreateParams, RunStatus, ToolHandlerResult
from purra.errors import ContractViolationError
from .codec import dump_storage_value, load_storage_value
from .journal import _BufferedEvents, SourceEvents

STORAGE_STATE_SCHEMA = "purra.storage-state/python/v1"


@dataclass(frozen=True)
class StorageRunInfo:
    run_id: str
    root_run_id: str
    status: RunStatus
    has_checkpoint: bool
    model_attempt_count: int
    checkpoint_attempt_count: int


class StorageSession:
    def __init__(self, body: str | None = None):
        self._adapters = InMemoryAgentAdapters()
        for name in STORAGE_PORT_METHODS:
            setattr(self, name, getattr(self._adapters, name))
        self.claims = {}
        self.leases = {}
        self.extra = {}
        if body is not None:
            saved = load_storage_value(body)
            if not isinstance(saved, dict) or set(saved) != {"schema", "groups", "claims", "leases", "extra"} or saved["schema"] != STORAGE_STATE_SCHEMA:
                raise ValueError("unsupported Core storage state")
            groups = saved["groups"]
            if not isinstance(groups, dict) or set(groups) != set(_FIELDS):
                raise ValueError("invalid storage groups")
            for name, obj in self._groups().items():
                values = groups[name]
                if not isinstance(values, dict) or set(values) != set(_FIELDS[name]):
                    raise ValueError("invalid storage fields")
                for key, field in _FIELDS[name].items():
                    value = values[key]
                    expected = getattr(obj, field)
                    if type(value) is not type(expected) or (type(expected) is int and value < 0):
                        raise ValueError("invalid storage field type")
                    setattr(obj, field, value)
            for name in ("claims", "leases", "extra"):
                if not isinstance(saved[name], dict):
                    raise ValueError("invalid storage extension")
                setattr(self, name, saved[name])
            self._validate_runs()
        self._prior_sequences = dict(self._state.sequences)
        self._prior_runs = set(self._state.runs)

    @property
    def _state(self):
        return self._adapters.runs._state

    def _groups(self):
        return {"run": self._state, "tree": self._adapters.run_tree,
                "artifact": self._adapters.artifacts, "task": self._adapters.long_tasks}

    def _validate_runs(self):
        from purra.adapters.memory import _RunRecord, _StreamRecord
        for run_id, record in self._state.runs.items():
            if not isinstance(run_id, str) or not isinstance(record, _RunRecord) or not isinstance(record.params, RunCreateParams) or not isinstance(record.status, RunStatus):
                raise ValueError("invalid stored Run")
            root = record.params.root_run_id or run_id
            if root not in self._state.runs:
                raise ValueError("missing stored Root")
        for sequence_map in (self._state.sequences, self._state.root_sequences, self._state.published_sequences):
            if any(key not in self._state.runs or type(value) is not int or value < 0 for key, value in sequence_map.items()):
                raise ValueError("invalid stored sequence")
        if any(not isinstance(v, _StreamRecord) or v.spec.run_id not in self._state.runs for v in self._state.streams.values()):
            raise ValueError("invalid stored stream")

    def export_snapshot(self) -> str:
        groups = {name: {key: getattr(obj, field) for key, field in _FIELDS[name].items()}
                  for name, obj in self._groups().items()}
        return dump_storage_value({"schema": STORAGE_STATE_SCHEMA, "groups": groups,
                                   "claims": self.claims, "leases": self.leases, "extra": self.extra})

    def get_run_info(self, run_id):
        run = self._state.runs.get(run_id)
        return None if run is None else StorageRunInfo(run_id, run.params.root_run_id or run_id,
            run.status, run.execution_checkpoint is not None, len(run.model_attempt_ids), run.checkpoint_attempt_count)

    def root_for_run(self, run_id):
        info = self.get_run_info(run_id)
        return info.root_run_id if info is not None else None

    def run_for_stream(self, stream_id):
        stream = self._state.streams.get(stream_id)
        return stream.spec.run_id if stream is not None else None

    def find_tree_run(self, run_id):
        return self._adapters.run_tree._runs.get(run_id)

    def running_run_ids(self):
        return tuple(key for key, run in self._state.runs.items() if run.status is RunStatus.RUNNING)

    def get_tool_receipt(self, key):
        return self._state.tool_receipts.get(key)

    def save_tool_receipt(self, key, call, result):
        if not isinstance(result, ToolHandlerResult):
            raise TypeError("invalid tool result")
        previous = self.get_tool_receipt(key)
        if previous is not None and previous != (call, result):
            raise ContractViolationError("tool receipt conflicts", code="tool_idempotency_conflict")
        self._state.tool_receipts[key] = (call, result)

    def restore_output_events(self, events, *, root_run_id=None, find_source=None):
        state = self._state
        if find_source is not None:
            state.events_by_source_key = SourceEvents(find_source)
        for event in events:
            if event.run_id not in state.runs or self.root_for_run(event.run_id) != event.root_run_id:
                raise ValueError("output journal references a missing Run or wrong Root")
            if event.sequence != len(state.output_events.get(event.run_id, ())) + 1 or event.root_sequence != len(state.root_output_events.get(event.root_run_id, ())) + 1:
                raise ValueError("invalid output journal sequence")
            state.output_events.setdefault(event.run_id, []).append(event)
            state.root_output_events.setdefault(event.root_run_id, []).append(event)
            state.events_by_source_key[event.source_event_key] = event
        if any(len(state.output_events.get(run_id, ())) != sequence
               for run_id, sequence in state.sequences.items()
               if root_run_id is None or self.root_for_run(run_id) == root_run_id):
            raise ValueError("incomplete output journal")

    def defer_output_events(self, root_run_id, counts, *, load_run, load_root, find_source):
        state = self._state
        if any(self.root_for_run(run_id) != root_run_id for run_id in counts):
            raise ValueError("output journal references a missing Run or wrong Root")
        total = sum(counts.values())
        if total != state.root_sequences.get(root_run_id, 0):
            raise ValueError("incomplete output journal")
        for run_id in state.runs:
            if self.root_for_run(run_id) != root_run_id:
                continue
            count = counts.get(run_id, 0)
            if count != state.sequences.get(run_id, 0):
                raise ValueError("incomplete output journal")
            state.output_events[run_id] = _BufferedEvents(count, lambda run_id=run_id: load_run(run_id))
        state.root_output_events[root_run_id] = _BufferedEvents(total, load_root)
        state.events_by_source_key = SourceEvents(find_source)

    def output_delta(self):
        roots = {run_id: self.root_for_run(run_id) for run_id in self._state.runs.keys() - self._prior_runs}
        events = tuple(event for run_id, history in self._state.output_events.items()
                       for event in history[self._prior_sequences.get(run_id, 0):])
        return roots, events

_FIELDS = {
    "run": {
        "runs": "runs",
        "streams": "streams",
        "stream_by_invocation": "stream_by_invocation",
        "sequences": "sequences",
        "root_sequences": "root_sequences",
        "published_sequences": "published_sequences",
        "tool_receipts": "tool_receipts",
        "run_count": "run_count",
    },
    "tree": {
        "agents": "_agents",
        "runs": "_runs",
        "checkpoints": "_checkpoints",
        "spawn_receipts": "_spawn_receipts",
        "continue_receipts": "_continue_receipts",
        "root_digests": "_root_digests",
        "sequence": "_sequence",
        "agent_sequence": "_agent_sequence",
        "run_sequence": "_run_sequence",
        "batch_sequence": "_batch_sequence",
        "checkpoint_sequence": "_checkpoint_sequence",
    },
    "artifact": {
        "artifacts": "_artifacts",
        "owners": "_owners",
        "batches": "_batches",
        "receipts": "_receipts",
        "receipt_digests": "_receipt_digests",
        "claims": "_claims",
        "updated_at_ms": "_updated_at_ms",
    },
    "task": {
        "tasks": "_tasks",
    },
}

STORAGE_PORT_METHODS = {
    "runs": frozenset(('append_event', 'append_trace', 'begin', 'bind_conversation', 'commit', 'get', 'reserve_model_attempt', 'settle_model_attempt')),
    "outputs": frozenset(('abort_stream', 'append_batch', 'append_event', 'begin_run_lifecycle', 'commit_run_lifecycle', 'commit_stream', 'list_events', 'list_root_events', 'load_validated_result', 'open_stream', 'publish_stream_content_as_commentary', 'publish_stream_content_as_final')),
    "run_tree": frozenset(('aggregate_runs', 'begin_root', 'cancel_subtree', 'claim_run', 'close_agent', 'complete_run', 'continue_agent', 'fail_run', 'get_agent', 'get_checkpoint', 'get_run', 'list_descendants', 'list_runnable', 'mark_waiting', 'release_waiting', 'renew_run_lease', 'require_run_claim', 'spawn_agents', 'suspend_run')),
    "artifacts": frozenset(('abort', 'acquire', 'append', 'create', 'finalize', 'find_for_owner', 'inspect', 'list_batches', 'load', 'load_active', 'maintain', 'release', 'release_for_run', 'renew', 'replay_receipt')),
    "artifact_claims": frozenset(('abort', 'acquire', 'append', 'create', 'finalize', 'find_for_owner', 'inspect', 'list_batches', 'load', 'load_active', 'maintain', 'release', 'release_for_run', 'renew', 'replay_receipt')),
    "artifact_maintenance": frozenset(('abort', 'acquire', 'append', 'create', 'finalize', 'find_for_owner', 'inspect', 'list_batches', 'load', 'load_active', 'maintain', 'release', 'release_for_run', 'renew', 'replay_receipt')),
    "long_tasks": frozenset(('bind_run', 'bind_unit_run', 'cancel', 'claim_ready_unit', 'complete_unit', 'create', 'expand_unit', 'expire_deadline', 'finalize_if_complete', 'find_active', 'find_by_idempotency_key', 'interrupt_unit', 'list_for_owner', 'list_run_bindings', 'list_units', 'load', 'pause', 'record_usage', 'recover_after_restart', 'renew_unit_lease', 'request_cancel', 'resume', 'settle_unit_failure', 'start', 'update_unit_progress')),
}
