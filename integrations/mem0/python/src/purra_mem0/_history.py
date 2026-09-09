"""History resource ownership for the optional, pinned Mem0 SDK."""

from mem0.memory.storage import SQLiteManager as _SQLiteManager


class SQLiteManager(_SQLiteManager):
    def __init__(self, db_path=":memory:"):
        # The upstream finalizer also runs when sqlite3.connect fails.
        self.connection = None
        try:
            super().__init__(db_path)
        except BaseException:
            try:
                self.close()
            except Exception:
                pass  # Preserve the original initialization failure.
            raise
