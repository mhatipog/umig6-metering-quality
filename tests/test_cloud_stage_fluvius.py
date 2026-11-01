"""Stage only pinned public archives, with bounded ZIP-member streaming."""
import io
import hashlib
import pytest

from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "examples"))
from cloud_stage_fluvius import _stream_copy, stage, EXPECTED_ARCHIVES

def test_archive_hashes_are_pinned():
    assert len(EXPECTED_ARCHIVES) == 2
    assert all(len(x) == 64 for x in EXPECTED_ARCHIVES)

def test_stream_copy_validates_hash_and_limit():
    raw = b"a" * 4096
    output = io.BytesIO()
    count, digest = _stream_copy(io.BytesIO(raw), output, limit=4096)
    assert count == len(raw)
    assert digest == hashlib.sha256(raw).hexdigest()
    assert output.getvalue() == raw
    with pytest.raises(ValueError):
        _stream_copy(io.BytesIO(raw), io.BytesIO(), limit=1024)

def test_staging_rejects_arbitrary_directories_and_archives(tmp_path):
    with pytest.raises(ValueError):
        stage(str(tmp_path / "umig6-p6269-2024"), archive_indices=(0,))
    with pytest.raises(ValueError):
        stage("/Volumes/test/default/x/umig6-p6269-2024", archive_indices=(0,0))
    with pytest.raises(ValueError):
        stage("/lakehouse/default/Files/umig6-p6269-2024", archive_indices=(2,))
