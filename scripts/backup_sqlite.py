"""Copy a PurrA SQLite snapshot to a new file; never replace a live database.

Stop application writers before using a snapshot as an upgrade rollback point.
A consistent backup of active work does not make that work safe to resume.
Uses only Python's standard library and preserves either SDK's storage bytes.
"""
from __future__ import annotations

from contextlib import closing
import argparse
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import tempfile


def _version(db: sqlite3.Connection) -> int:
    marker = db.execute("SELECT version,body FROM purra_state WHERE scope='' AND sdk='purra.approvals'").fetchone()
    version = 5 if marker == (5, '{}') else 4
    if marker is not None and version != 5:
        raise ValueError('unsupported SQLite storage version')
    if db.execute('SELECT 1 FROM purra_state WHERE version != ? LIMIT 1', (version,)).fetchone():
        raise ValueError('unsupported SQLite storage version')
    return version


def backup_sqlite(source: Path, destination: Path) -> dict:
    if os.name != "posix":
        raise ValueError("backup utility requires a POSIX host")
    source = source.resolve(strict=True)
    destination = destination.absolute()
    if destination.exists() or destination.is_symlink():
        raise FileExistsError('backup destination already exists')
    # A private staging file avoids exposing a partial snapshot as the destination.
    fd, staging_name = tempfile.mkstemp(prefix='.purra-backup-', dir=destination.parent)
    os.close(fd)
    staging = Path(staging_name)
    try:
        with closing(sqlite3.connect(source.as_uri() + '?mode=ro', uri=True)) as original:
            target = sqlite3.connect(staging)
            try:
                original.backup(target)
                if target.execute('PRAGMA quick_check').fetchall() != [('ok',)]:
                    raise ValueError('backup integrity check failed')
                if target.execute('PRAGMA foreign_key_check').fetchone() is not None:
                    raise ValueError('backup foreign key check failed')
                version = _version(target)
            finally:
                target.close()
        with staging.open('rb') as copied:
            digest = hashlib.file_digest(copied, 'sha256').hexdigest()
            os.fsync(copied.fileno())
        # Atomic publication that also rejects a destination created during backup.
        size = staging.stat().st_size
        os.link(staging, destination)
        staging.unlink()
        directory_fd = os.open(destination.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        return {'schemaVersion': 1, 'storageVersion': version, 'sha256': digest,
                'bytes': size, 'resumeAuthority': 'none'}
    finally:
        staging.unlink(missing_ok=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('source', type=Path)
    parser.add_argument('destination', type=Path)
    args = parser.parse_args()
    print(json.dumps(backup_sqlite(args.source, args.destination), sort_keys=True))


if __name__ == '__main__':
    main()
