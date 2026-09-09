"""Validated, transaction-local extension access without repeated record reconstruction."""
import json

from .codec import _decode, _encode, _object
from .session import StorageSession


class StorageMetadataCache:
    """One-entry validation cache, not a cache of current database state.

    Every call must receive the body read in the current storage transaction.
    Unchanged encoded non-extension state reuses validation only; extensions are
    decoded afresh. A changed Run, claim, lease or schema requires full validation.
    """

    def __init__(self):
        self._validated = None
        self._has_checkpoint = False

    def open(self, body: str | None):
        if body is None:
            body = StorageSession().export_snapshot()
        try:
            tree = json.loads(body, object_pairs_hook=_object)
            if not isinstance(tree, list) or len(tree) != 2 or tree[0] != 'map' or not isinstance(tree[1], list):
                raise ValueError('invalid storage envelope')
            fields = {}
            for pair in tree[1]:
                if not isinstance(pair, list) or len(pair) != 2:
                    raise ValueError('invalid storage envelope')
                key = _decode(pair[0], 1)
                if not isinstance(key, str) or key in fields:
                    raise ValueError('invalid storage envelope')
                fields[key] = pair
            if set(fields) != {'schema', 'groups', 'claims', 'leases', 'extra'}:
                raise ValueError('invalid storage envelope')
            # JSON text distinguishes e.g. true, 1 and 1.0; Python equality does not.
            validation_key = json.dumps([fields[key][1] for key in ('schema', 'groups', 'claims', 'leases')],
                ensure_ascii=False, allow_nan=False, separators=(',', ':'))
            extra = _decode(fields['extra'][1], 1)
            if not isinstance(extra, dict):
                raise ValueError('invalid storage extension')
            if validation_key != self._validated:
                session = StorageSession(body)
                has_checkpoint = session.has_tool_ready_checkpoint()
                self._validated = validation_key
                self._has_checkpoint = has_checkpoint
            return StorageMetadataSession(tree, fields['extra'], extra, self._has_checkpoint)
        except (KeyError, TypeError, OverflowError, RecursionError) as error:
            raise ValueError('invalid storage metadata') from error


class StorageMetadataSession:
    """Only extension state is mutable; canonical encoded state stays untouched."""

    def __init__(self, tree, extra_pair, extra, has_checkpoint):
        self._tree = tree
        self._extra_pair = extra_pair
        self.extra = extra
        self._has_checkpoint = has_checkpoint

    def has_tool_ready_checkpoint(self):
        return self._has_checkpoint

    def export_snapshot(self):
        if not isinstance(self.extra, dict):
            raise ValueError('invalid storage extension')
        self._extra_pair[1] = _encode(self.extra)
        return json.dumps(self._tree, ensure_ascii=False, allow_nan=False, separators=(',', ':'))
