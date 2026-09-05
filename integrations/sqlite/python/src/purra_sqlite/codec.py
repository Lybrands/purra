"""Data-only encoding of the version-pinned Core storage records."""
from collections.abc import Mapping, Sequence
from dataclasses import fields, is_dataclass
from datetime import datetime
from enum import Enum
import json
import math
import sys


def _types():
    # Only already imported Core record classes are allowed; data cannot import code.
    return {f"{value.__module__}.{value.__qualname__}": value
            for name, module in tuple(sys.modules.items()) if name.startswith("purra.")
            for value in tuple(vars(module).values())
            if isinstance(value, type) and value.__module__.startswith("purra.")
            and (is_dataclass(value) or issubclass(value, Enum))}


def encode(value):
    if isinstance(value, Enum):
        return ["enum", f"{type(value).__module__}.{type(value).__qualname__}", value.value]
    if value is None or type(value) in (str, int, bool):
        return ["value", value]
    if type(value) is float and math.isfinite(value):
        return ["value", value]
    if isinstance(value, datetime):
        return ["datetime", value.isoformat()]
    if is_dataclass(value) and not isinstance(value, type):
        return ["record", f"{type(value).__module__}.{type(value).__qualname__}",
                {f.name: encode(getattr(value, f.name)) for f in fields(value) if f.init}]
    if isinstance(value, Mapping):
        return ["map", [[encode(k), encode(v)] for k, v in value.items()]]
    if isinstance(value, (list, tuple, set, frozenset)):
        return [type(value).__name__, [encode(v) for v in value]]
    if isinstance(value, Sequence):
        return ["list", [encode(v) for v in value]]
    raise TypeError(f"Unsupported durable value: {type(value).__name__}")


def dumps(value):
    return json.dumps(encode(value), ensure_ascii=False, allow_nan=False, separators=(",", ":"))


def loads(text):
    return next(load_many((text,)))


def load_many(texts):
    registry = None
    def decode(row):
        kind = row[0]
        if kind == "value": return row[1]
        if kind == "datetime": return datetime.fromisoformat(row[1])
        if kind == "enum": return registry[row[1]](row[2])
        if kind == "record": return registry[row[1]](**{k: decode(v) for k, v in row[2].items()})
        if kind == "map": return {decode(k): decode(v) for k, v in row[1]}
        constructors = {"list": list, "tuple": tuple, "set": set, "frozenset": frozenset}
        return constructors[kind](decode(v) for v in row[1])
    for text in texts:
        if registry is None:
            registry = _types()
        yield decode(json.loads(text))
