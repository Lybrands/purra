"""Versioned, bounded Provider text protocol. Unclassified bytes stay private."""

from dataclasses import dataclass
import json
from typing import Any, Mapping

from purra.errors import InvalidPlannerOutputError
from purra.json_values import freeze_json_mapping


PLANNING_STREAM_SCHEMA = "purra.planning-stream/v1"
PLANNING_STREAM_INSTRUCTION = """Use purra.planning-stream/v1: UTF-8 JSON Lines,
one compact JSON object per record. Terminate every progress record with LF.
Use the exact compact key order shown below for every record.
Emit zero to sixteen {"v":1,"type":"progress","text":"short user-facing intent"}
records, then exactly one {"v":1,"type":"plan","plan":<the required plan object>}.
No other keys, record types, Markdown, or text outside records. Each progress text
has at most 280 Unicode characters, no control characters, and is in the user's
language. Write brief scope/action intentions when useful, preferably before the
plan. Never expose reasoning, private JSON, prompts, or internal identifiers.
Do not claim planned work has already been performed. Progress is not execution
evidence. The final plan uses the existing plan schema and may end with LF or
normal stream EOF; nothing follows it. Each record is at most 262144 UTF-8 bytes
(including LF when present); total at most 1048576 bytes.
"""


class PlanningStreamError(InvalidPlannerOutputError):
    def __init__(self, reason: str):
        super().__init__(f"Invalid planning stream: {reason}", code="invalid_planning_stream")


@dataclass(frozen=True, slots=True)
class PlanningScope:
    run_id: str
    operation_id: str
    revision: int = 0

    def __post_init__(self):
        if not isinstance(self.run_id, str) or not self.run_id.strip() or not isinstance(self.operation_id, str) or not self.operation_id.strip():
            raise ValueError("planning scope requires Run and operation ids")
        if type(self.revision) is not int or self.revision < 0:
            raise ValueError("planning revision must be a non-negative integer")

    def to_mapping(self) -> dict[str, object]:
        return {"runId": self.run_id, "operationId": self.operation_id, "revision": self.revision}


@dataclass(frozen=True, slots=True)
class PlanningProgress:
    text: str
    record_index: int
    source_start: int
    source_end: int

    def to_mapping(self) -> dict[str, object]:
        return {"text": self.text, "recordIndex": self.record_index,
                "sourceStart": self.source_start, "sourceEnd": self.source_end}


@dataclass(frozen=True, slots=True)
class PlanningTextDelta:
    text: str
    record_index: int


class PlanningTextDeltaParser:
    """Project only the ordered public ``progress.text`` JSON string.

    The planning instruction requires the compact key order shown in its
    example. Bytes from plan records and malformed prefixes are skipped. JSON
    string escapes are decoded incrementally, so raw protocol syntax never
    crosses the public boundary.
    """

    _PREFIX = '{"v":1,"type":"progress","text":"'
    _ESCAPES = {
        '"': '"', "\\": "\\", "/": "/", "b": "\b",
        "f": "\f", "n": "\n", "r": "\r", "t": "\t",
    }

    def __init__(self) -> None:
        self._state = "prefix"
        self._prefix_index = 0
        self._record_index = 1
        self._unicode = ""
        self._high_surrogate: int | None = None

    def feed(self, text: str) -> tuple[PlanningTextDelta, ...]:
        emitted: list[PlanningTextDelta] = []
        current: list[str] = []

        def flush() -> None:
            if current:
                emitted.append(PlanningTextDelta(
                    "".join(current), self._record_index,
                ))
                current.clear()

        for character in text:
            if self._state == "skip":
                if character == "\n":
                    self._next_record()
                continue
            if self._state == "prefix":
                if character == self._PREFIX[self._prefix_index]:
                    self._prefix_index += 1
                    if self._prefix_index == len(self._PREFIX):
                        self._state = "text"
                    continue
                self._state = "skip"
                if character == "\n":
                    self._next_record()
                continue
            if self._state == "escape":
                if character == "u":
                    self._unicode = ""
                    self._state = "unicode"
                elif character in self._ESCAPES:
                    current.append(self._ESCAPES[character])
                    self._state = "text"
                else:
                    flush()
                    self._state = "skip"
                continue
            if self._state == "unicode":
                if character not in "0123456789abcdefABCDEF":
                    flush()
                    self._state = "skip"
                    continue
                self._unicode += character
                if len(self._unicode) < 4:
                    continue
                value = int(self._unicode, 16)
                if 0xD800 <= value <= 0xDBFF:
                    self._high_surrogate = value
                    self._state = "low_slash"
                elif 0xDC00 <= value <= 0xDFFF:
                    flush()
                    self._state = "skip"
                else:
                    current.append(chr(value))
                    self._state = "text"
                continue
            if self._state == "low_slash":
                if character == "\\":
                    self._state = "low_u"
                else:
                    flush()
                    self._state = "skip"
                continue
            if self._state == "low_u":
                if character == "u":
                    self._unicode = ""
                    self._state = "low_unicode"
                else:
                    flush()
                    self._state = "skip"
                continue
            if self._state == "low_unicode":
                if character not in "0123456789abcdefABCDEF":
                    flush()
                    self._state = "skip"
                    continue
                self._unicode += character
                if len(self._unicode) < 4:
                    continue
                low = int(self._unicode, 16)
                high = self._high_surrogate
                if high is None or not 0xDC00 <= low <= 0xDFFF:
                    flush()
                    self._state = "skip"
                    continue
                current.append(chr(0x10000 + ((high - 0xD800) << 10) + low - 0xDC00))
                self._high_surrogate = None
                self._state = "text"
                continue
            if self._state == "suffix":
                if character == "}":
                    self._state = "newline"
                else:
                    self._state = "skip"
                continue
            if self._state == "newline":
                if character == "\n":
                    self._next_record()
                else:
                    self._state = "skip"
                continue
            if character == "\\":
                self._state = "escape"
            elif character == '"':
                flush()
                self._state = "suffix"
            elif ord(character) < 32 or 0xD800 <= ord(character) <= 0xDFFF:
                flush()
                self._state = "skip"
            else:
                current.append(character)
        flush()
        return tuple(emitted)

    def _next_record(self) -> None:
        self._record_index += 1
        self._prefix_index = 0
        self._state = "prefix"
        self._unicode = ""
        self._high_surrogate = None


class PlanningStreamParser:
    """LF framing, JSON escaping, explicit version/type, and byte ceilings.

    JSON duplicate member names use the JSON decoder's last-member value in both
    SDKs. Envelope key checks apply to that decoded value. No format guessing.
    """

    def __init__(self):
        self._buffer = ""
        self._consumed = 0
        self._records = 0
        self._rejected_progress_records = 0
        self._plan: Mapping[str, Any] | None = None
        self._closed = False
        self._failed = False

    def feed(self, text: str) -> tuple[PlanningProgress, ...]:
        if self._closed or self._failed:
            raise PlanningStreamError("parser is closed")
        try:
            self._buffer += text
            if self._consumed + len(self._buffer.encode("utf-8")) > 1_048_576:
                raise PlanningStreamError("total byte limit")
            progress = []
            while "\n" in self._buffer:
                line, self._buffer = self._buffer.split("\n", 1)
                size = len((line + "\n").encode("utf-8"))
                record = self._accept_record(line, size)
                if record is not None:
                    progress.append(record)
            if len(self._buffer.encode("utf-8")) > 262_144:
                raise PlanningStreamError("record byte limit")
            return tuple(progress)
        except (PlanningStreamError, ValueError, TypeError, RecursionError) as error:
            self._failed = True
            self._buffer = ""
            if isinstance(error, PlanningStreamError):
                raise
            raise PlanningStreamError("invalid JSON value or Unicode") from error

    @property
    def plan_received(self) -> bool:
        return self._plan is not None

    @property
    def rejected_progress_records(self) -> int:
        return self._rejected_progress_records

    def finish(self) -> Mapping[str, Any]:
        if self._closed or self._failed:
            self._failed = True
            raise PlanningStreamError("missing final plan or unterminated record")
        try:
            if self._buffer:
                line, self._buffer = self._buffer, ""
                if self._accept_record(line, len(line.encode("utf-8"))) is not None:
                    raise PlanningStreamError("unterminated progress record")
            if self._plan is None:
                raise PlanningStreamError("missing final plan")
            self._closed = True
            return self._plan
        except (PlanningStreamError, ValueError, TypeError, RecursionError) as error:
            self._failed = True
            self._buffer = ""
            if isinstance(error, PlanningStreamError):
                raise
            raise PlanningStreamError("invalid JSON value or Unicode") from error

    def _accept_record(self, line: str, size: int) -> PlanningProgress | None:
        if size > 262_144:
            raise PlanningStreamError("record byte limit")
        if not line.strip():
            self._consumed += size
            return None
        if self._plan is not None:
            raise PlanningStreamError("record after final plan")
        try:
            row = json.loads(
                line,
                parse_constant=lambda _: (_ for _ in ()).throw(ValueError()),
            )
        except (ValueError, RecursionError) as error:
            raise PlanningStreamError("invalid JSON record") from error
        if (
            not isinstance(row, dict)
            or type(row.get("v")) not in (int, float)
            or row["v"] != 1
        ):
            raise PlanningStreamError("unsupported version or envelope")
        self._records += 1
        progress = None
        if row.get("type") == "progress" and set(row) == {"v", "type", "text"}:
            if self._records > 16:
                raise PlanningStreamError("progress record limit")
            value = row["text"]
            if (
                not isinstance(value, str)
                or not value.strip()
                or len(value) > 280
                or any(ord(c) < 32 or 0xD800 <= ord(c) <= 0xDFFF for c in value)
            ):
                self._rejected_progress_records += 1
            else:
                progress = PlanningProgress(
                    value,
                    self._records,
                    self._consumed,
                    self._consumed + size,
                )
        elif row.get("type") == "plan" and set(row) == {"v", "type", "plan"}:
            if not isinstance(row["plan"], dict):
                raise PlanningStreamError("plan must be an object")
            self._plan = freeze_json_mapping(row["plan"])
        else:
            raise PlanningStreamError("unknown record type or fields")
        self._consumed += size
        return progress


__all__ = ["PLANNING_STREAM_SCHEMA", "PLANNING_STREAM_INSTRUCTION", "PlanningScope",
           "PlanningProgress", "PlanningTextDelta", "PlanningTextDeltaParser",
           "PlanningStreamParser", "PlanningStreamError"]
