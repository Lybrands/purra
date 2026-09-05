"""Data-only codec with explicit, location-independent Core record identities."""
import json
import math
from collections.abc import Mapping, Sequence
from datetime import datetime
from enum import Enum

from ._records import ENUMS, RECORDS

_RECORD_IDS = {cls: (name, names) for name, (cls, names) in RECORDS.items()}
_ENUM_IDS = {cls: name for name, cls in ENUMS.items()}


def _encode(value):
    if isinstance(value, Enum):
        if type(value) not in _ENUM_IDS:
            raise ValueError("unsupported storage enum")
        return ["enum", _ENUM_IDS[type(value)], value.value]
    if value is None or type(value) in (str, int, bool):
        return ["value", value]
    if type(value) is float and math.isfinite(value):
        return ["value", value]
    if isinstance(value, datetime):
        return ["datetime", value.isoformat()]
    if type(value) in _RECORD_IDS:
        name, names = _RECORD_IDS[type(value)]
        return ["record", name, {key: _encode(getattr(value, key)) for key in names}]
    if isinstance(value, Mapping):
        return ["map", [[_encode(k), _encode(v)] for k, v in value.items()]]
    if isinstance(value, (list, tuple, set, frozenset)):
        return [type(value).__name__, [_encode(v) for v in value]]
    if isinstance(value, Sequence):
        return ["list", [_encode(v) for v in value]]
    raise ValueError("unsupported storage value")


def _decode(row, depth=0):
    if depth > 128 or not isinstance(row, list) or not row:
        raise ValueError("invalid storage value")
    kind = row[0]
    if not isinstance(kind, str) or len(row) != (3 if kind in ("record", "enum") else 2):
        raise ValueError("invalid storage encoding")
    if kind == "value":
        value = row[1]
        if value is None or type(value) in (str, int, bool) or (type(value) is float and math.isfinite(value)):
            return value
        raise ValueError("invalid storage scalar")
    if kind == "datetime":
        return datetime.fromisoformat(row[1])
    if kind == "enum":
        return ENUMS[row[1]](row[2])
    if kind == "record":
        cls, names = RECORDS[row[1]]
        if not isinstance(row[2], dict) or set(row[2]) != set(names):
            raise ValueError("invalid storage record fields")
        return cls(**{key: _decode(row[2][key], depth + 1) for key in names})
    if kind == "map":
        if not isinstance(row[1], list):
            raise ValueError("invalid storage map")
        result = {}
        for pair in row[1]:
            if not isinstance(pair, list) or len(pair) != 2:
                raise ValueError("invalid storage pair")
            key, value = (_decode(v, depth + 1) for v in pair)
            if key in result:
                raise ValueError("duplicate storage key")
            result[key] = value
        return result
    constructors = {"list": list, "tuple": tuple, "set": set, "frozenset": frozenset}
    if kind not in constructors or not isinstance(row[1], list):
        raise ValueError("invalid storage collection")
    return constructors[kind](_decode(v, depth + 1) for v in row[1])


def dump_storage_value(value) -> str:
    return json.dumps(_encode(value), ensure_ascii=False, allow_nan=False, separators=(",", ":"))


def _object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate storage field")
        result[key] = value
    return result


def load_storage_value(text: str):
    try:
        return _decode(json.loads(text, object_pairs_hook=_object))
    except (KeyError, TypeError, OverflowError, RecursionError) as error:
        raise ValueError("invalid storage value") from error


def load_storage_values(texts):
    for text in texts:
        yield load_storage_value(text)
