"""Immutable JSON snapshots for PurrA boundary contracts.

Frozen containers deliberately do not inherit from ``dict`` or ``list``.
Builtin base-class mutators bypass overrides on subclasses, so subclassing
would make the immutability boundary advisory rather than real.  Adapters must
call :func:`thaw_json_value` before handing values to a JSON encoder/provider.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Iterator, Mapping, Sequence
from types import MappingProxyType
from typing import Any, cast


class FrozenDict(Mapping[str, Any]):
    """A detached read-only mapping backed by ``MappingProxyType``."""

    __slots__ = ("__data",)

    def __init__(self, value: Mapping[str, Any]):
        object.__setattr__(self, "_FrozenDict__data", MappingProxyType(dict(value)))

    def __getitem__(self, key: str) -> Any:
        return self.__data[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self.__data)

    def __len__(self) -> int:
        return len(self.__data)

    def __repr__(self) -> str:
        return f"FrozenDict({dict(self.__data)!r})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Mapping):
            return NotImplemented
        return len(self) == len(other) and all(
            key in other and value == other[key]
            for key, value in self.items()
        )

    @staticmethod
    def _immutable(*_args, **_kwargs):
        raise TypeError("frozen JSON mapping is immutable")

    __setitem__ = _immutable
    __delitem__ = _immutable
    clear = _immutable
    pop = _immutable
    popitem = _immutable
    setdefault = _immutable
    update = _immutable
    __ior__ = _immutable

    def __deepcopy__(self, _memo):
        return self


class FrozenList(Sequence[Any]):
    """A detached read-only sequence backed by a tuple."""

    __slots__ = ("__items",)

    def __init__(self, value):
        object.__setattr__(self, "_FrozenList__items", tuple(value))

    def __getitem__(self, index):
        return self.__items[index]

    def __iter__(self) -> Iterator[Any]:
        return iter(self.__items)

    def __len__(self) -> int:
        return len(self.__items)

    def __repr__(self) -> str:
        return f"FrozenList({list(self.__items)!r})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Sequence) or isinstance(
            other,
            (str, bytes, bytearray),
        ):
            return NotImplemented
        return len(self) == len(other) and all(
            left == right for left, right in zip(self, other)
        )

    @staticmethod
    def _immutable(*_args, **_kwargs):
        raise TypeError("frozen JSON sequence is immutable")

    __setitem__ = _immutable
    __delitem__ = _immutable
    append = _immutable
    clear = _immutable
    extend = _immutable
    insert = _immutable
    pop = _immutable
    remove = _immutable
    reverse = _immutable
    sort = _immutable
    __iadd__ = _immutable
    __imul__ = _immutable

    def __deepcopy__(self, _memo):
        return self


def freeze_json_value(value: Any, *, _active: set[int] | None = None) -> Any:
    """Return a detached, recursively immutable JSON value.

    Non-JSON objects, non-string mapping keys, cyclic containers and non-finite
    floats are rejected at the Core boundary instead of being stringified.
    """

    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("JSON numbers must be finite")
        return value
    if isinstance(value, (FrozenDict, FrozenList)):
        return value

    active = _active if _active is not None else set()
    if isinstance(value, Mapping):
        identity = id(value)
        if identity in active:
            raise ValueError("cyclic JSON mapping is not supported")
        active.add(identity)
        try:
            rows: dict[str, Any] = {}
            for key, item in value.items():
                if not isinstance(key, str):
                    raise TypeError("JSON mapping keys must be strings")
                rows[key] = freeze_json_value(item, _active=active)
            return FrozenDict(rows)
        finally:
            active.remove(identity)

    if isinstance(value, (list, tuple)):
        identity = id(value)
        if identity in active:
            raise ValueError("cyclic JSON sequence is not supported")
        active.add(identity)
        try:
            return FrozenList(
                freeze_json_value(item, _active=active)
                for item in value
            )
        finally:
            active.remove(identity)

    raise TypeError(f"unsupported JSON value type: {type(value).__name__}")


def freeze_json_mapping(value: Mapping[str, Any] | None = None) -> FrozenDict:
    return cast(FrozenDict, freeze_json_value({} if value is None else value))


def thaw_json_value(value: Any) -> Any:
    """Return a detached mutable JSON value suitable for an adapter."""

    if isinstance(value, Mapping):
        return {str(key): thaw_json_value(item) for key, item in value.items()}
    if isinstance(value, (FrozenList, list, tuple)):
        return [thaw_json_value(item) for item in value]
    return value


def thaw_json_mapping(value: Mapping[str, Any]) -> dict[str, Any]:
    return {str(key): thaw_json_value(item) for key, item in value.items()}


def canonical_json_digest(value: Any) -> str:
    encoded = json.dumps(
        thaw_json_value(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()
