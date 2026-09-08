"""The standalone backup utility must preserve committed WAL data and never overwrite."""
import importlib.util
from pathlib import Path
import sqlite3

import pytest

spec = importlib.util.spec_from_file_location('backup_sqlite', Path(__file__).parents[1] / 'scripts/backup_sqlite.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


@pytest.mark.parametrize('sdk', ['python', 'typescript'])
@pytest.mark.parametrize('version', [4, 5])
def test_backup_preserves_wal_and_requires_new_restore_path(tmp_path, sdk, version):
    source, backup, restored = (tmp_path / name for name in ('source.db', 'backup.db', 'restored.db'))
    db = sqlite3.connect(source)
    try:
        db.execute('PRAGMA journal_mode=WAL')
        db.execute('PRAGMA wal_autocheckpoint=0')
        db.execute('CREATE TABLE purra_state(scope TEXT,sdk TEXT,version INTEGER,body TEXT)')
        db.execute('INSERT INTO purra_state VALUES(?,?,?,?)', ('fixture', sdk, version, '{"history":"retained"}'))
        if version == 5:
            db.execute("INSERT INTO purra_state VALUES('','purra.approvals',5,'{}')")
        db.commit()
        expected = db.execute('SELECT * FROM purra_state').fetchall()
        assert Path(str(source) + '-wal').stat().st_size > 0
        report = module.backup_sqlite(source, backup)
        assert report['storageVersion'] == version and report['resumeAuthority'] == 'none'
        assert backup.stat().st_mode & 0o777 == 0o600
        with sqlite3.connect(backup) as copied:
            assert copied.execute('SELECT * FROM purra_state').fetchall() == expected
        with pytest.raises(FileExistsError):
            module.backup_sqlite(backup, source)
        module.backup_sqlite(backup, restored)
        with sqlite3.connect(restored) as copied:
            assert copied.execute('SELECT * FROM purra_state').fetchall() == expected
        assert db.execute('SELECT * FROM purra_state').fetchall() == expected
    finally:
        db.close()


def test_failed_backup_never_publishes_or_leaves_staging_files(tmp_path):
    source, target = tmp_path/'invalid.db', tmp_path/'backup.db'
    with sqlite3.connect(source) as db:
        db.execute('CREATE TABLE unrelated(value TEXT)')
    with pytest.raises(sqlite3.OperationalError):
        module.backup_sqlite(source, target)
    assert not target.exists()
    assert not list(tmp_path.glob('.purra-backup-*'))


def test_destination_symlink_is_not_followed(tmp_path):
    source = tmp_path/'source.db'
    source.touch()
    destination = tmp_path/'link.db'
    destination.symlink_to(tmp_path/'absent.db')
    with pytest.raises(FileExistsError):
        module.backup_sqlite(source, destination)
    assert not (tmp_path/'absent.db').exists()


def test_publication_race_preserves_other_writer_file(tmp_path, monkeypatch):
    source, target = tmp_path/'source.db', tmp_path/'backup.db'
    with sqlite3.connect(source) as db:
        db.execute('CREATE TABLE purra_state(scope TEXT,sdk TEXT,version INTEGER,body TEXT)')
    original_link = module.os.link
    def competing_link(staging, destination):
        destination.write_bytes(b'other writer')
        original_link(staging, destination)
    monkeypatch.setattr(module.os, 'link', competing_link)
    with pytest.raises(FileExistsError):
        module.backup_sqlite(source, target)
    assert target.read_bytes() == b'other writer'
    assert not list(tmp_path.glob('.purra-backup-*'))
