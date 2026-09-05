"""Deferred, transaction-local canonical output histories."""
import operator
from collections.abc import Sequence

class _BufferedEvents(Sequence):
    """Transaction-local history with an append buffer and deferred evidence reads."""

    def __init__(self, count, load):
        self.count, self.load = count, load
        self.pending = []
        self.history = None

    def __len__(self):
        return self.count + len(self.pending)

    def _history(self):
        if self.history is None:
            self.history = tuple(self.load())
            if len(self.history) != self.count:
                raise ValueError("incomplete output journal")
        return self.history

    def __iter__(self):
        yield from self._history()
        yield from self.pending

    def __getitem__(self, index):
        if isinstance(index, slice):
            start, stop, step = index.indices(len(self))
            if step > 0 and start >= self.count:
                return self.pending[start - self.count:stop - self.count:step]
            return tuple(self)[index]
        index = operator.index(index)
        if index < 0:
            index += len(self)
        if index < 0 or index >= len(self):
            raise IndexError(index)
        return self.pending[index - self.count] if index >= self.count else self._history()[index]

    def append(self, event):
        self.pending.append(event)


class SourceEvents(dict):
    def __init__(self, load):
        super().__init__()
        self.load = load

    def __missing__(self, key):
        event = self.load(key)
        if event is None:
            raise KeyError(key)
        self[key] = event
        return event

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default
