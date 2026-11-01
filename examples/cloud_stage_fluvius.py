"""Stage the original public Fluvius P6269 ZIP members into managed storage.

Caller supplies a pre-provisioned, disposable and writable mounted directory:
Fabric /lakehouse/default/Files/... or Databricks /Volumes/catalog/schema/volume/...
All eight CSVs are copied to persistent storage, not executor-local /tmp.
Only the official metadata API and archive URLs are accepted.

Never use this with private metering exports. Full-source reproducibility
requires exact ZIP SHA-256 and member checksums before Spark processing.
"""
from __future__ import annotations
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import tempfile
import time
import urllib.request
import zipfile

from umig6_metering_quality.public_profiles import DATASET_ID

BASE = "https://opendata.fluvius.be"
EXPECTED_ARCHIVES = {
    "0f8addd57bd9f7bc0fd0c28b019a4b6c8753a19e3fd301de758f5a5858c9d86c",
    "cfef37973c40e503fe53afa4966bbdcd7a12525fb8b20881f365d0ee8616a3d6",
}
HEADERS = {"User-Agent": "umig6-metering-quality-verified-research/1.0"}


def _get(url, timeout=35):
    if not url.startswith(BASE + "/api/explore/v2.1/"):
        raise ValueError("Only official Fluvius v2.1 API endpoints are allowed")
    return urllib.request.urlopen(urllib.request.Request(url, headers=HEADERS), timeout=timeout)


def official_attachments():
    url = BASE + "/api/explore/v2.1/catalog/datasets/" + DATASET_ID
    with _get(url) as r:
        metadata = json.load(r)
    attachments = [a for a in metadata.get("attachments", [])
                   if a.get("mimetype") == "application/zip"]
    if len(attachments) != 2:
        raise ValueError("Expected exactly two original P6269 ZIP attachments")
    for a in attachments:
        if not a.get("url", "").startswith(BASE + "/api/explore/v2.1/"):
            raise ValueError("Unexpected archive host or API route")
    return sorted(attachments, key=lambda a: a.get("title", ""))


def _stream_copy(src, dst, *, limit):
    total, digest = 0, hashlib.sha256()
    while True:
        chunk = src.read(2 * 1024 * 1024)
        if not chunk:
            break
        total += len(chunk)
        if total > limit:
            raise ValueError("Archive or CSV member exceeds documented size cap")
        digest.update(chunk)
        dst.write(chunk)
    return total, digest.hexdigest()


def stage(directory, *, archive_indices=(0, 1), max_archive_bytes=1_600_000_000):
    root = Path(directory)
    # No unbounded writes to a cloud home directory or arbitrary mount.
    candidate = str(root)
    if not ("umig6-p6269-2024" in candidate and
            (candidate.startswith("/lakehouse/default/Files/")
             or candidate.startswith("/Volumes/"))):
        raise ValueError("Provide a dedicated managed P6269 landing directory")
    if not archive_indices or len(set(archive_indices)) != len(archive_indices):
        raise ValueError("Choose unique archive indices")
    if any(i not in (0, 1) for i in archive_indices):
        raise ValueError("Only original archive indices 0 and 1 are allowed")
    root.mkdir(parents=True, exist_ok=True)
    attachments = official_attachments()
    archived = []
    members = []
    with tempfile.TemporaryDirectory(prefix="umig6-p6269-") as tmp:
        for index in archive_indices:
            attachment = attachments[index]
            source = Path(tmp) / ("source-" + str(index) + ".zip")
            started = time.perf_counter()
            with _get(attachment["url"], timeout=120) as incoming, source.open("wb") as target:
                compressed_bytes, checksum = _stream_copy(incoming, target, limit=max_archive_bytes)
            if checksum not in EXPECTED_ARCHIVES:
                raise ValueError("Official archive checksum differs from pinned October 2026 baseline")
            archive_entry = {"index": index, "compressed_bytes": compressed_bytes,
                             "sha256": checksum, "verified": True}
            with zipfile.ZipFile(source) as archive:
                entries = sorted([m for m in archive.infolist() if m.filename.lower().endswith(".csv")],
                                 key=lambda m: m.filename)
                if len(entries) != 4:
                    raise ValueError("Unexpected source ZIP layout; expected four CSV members")
                for part, info in enumerate(entries):
                    if not re.fullmatch(r"[^/\\]+\.csv", info.filename, flags=re.IGNORECASE):
                        raise ValueError("Nested or ambiguous CSV member not allowed")
                    if info.file_size > 1_000_000_000:
                        raise ValueError("CSV member exceeds 1 GB safety cap")
                    # Archive+ordinal makes names stable, regardless of user-controlled ZIP name.
                    target = root / ("source-%d-category-%d.csv" % (index, part))
                    partial = root / (target.name + ".partial")
                    if target.exists():
                        # Do not overwrite previously staged managed data silently.
                        raise FileExistsError("Staged category already exists: " + target.name)
                    try:
                        with archive.open(info) as input_file, partial.open("xb") as output:
                            bytes_written, digest = _stream_copy(
                                input_file, output, limit=1_000_000_000)
                        if bytes_written != info.file_size:
                            raise ValueError("Extracted CSV size does not match ZIP member")
                        os.replace(partial, target)
                    finally:
                        if partial.exists():
                            partial.unlink()
                    members.append({"archive_index": index, "member_index": part,
                                    "filename": target.name, "bytes": bytes_written,
                                    "sha256": digest})
            source.unlink()
            archive_entry["stage_seconds"] = round(time.perf_counter() - started, 3)
            archived.append(archive_entry)
    return {"format": "umig6-public-p6269-staging-v1", "dataset": DATASET_ID,
            "archive_checksums_verified": len(archived), "archives": archived,
            "members": members, "staged_csv_count": len(members),
            "paths": [str(root / m["filename"]) for m in members],
            "source_clock_utc_verified": False}


def sanitized(result):
    """Public metadata; never export storage locations or source measurements."""
    return {k:v for k,v in result.items() if k != "paths"}


def stage_verified_archive(archive_path, directory, *, archive_index):
    """Extract an already-uploaded original ZIP without internet on Spark nodes.

    For restricted Databricks serverless environments, the owner can transfer
    the pinned ZIP through UC Files APIs, then call this from a managed notebook.
    Re-verifies the compressed bytes *inside* that environment before reading
    a single input row. Does not invent a UTF/UTC mapping.
    """
    if archive_index not in (0, 1):
        raise ValueError("Unexpected original archive index")
    source = Path(archive_path)
    dest = Path(directory)
    if not str(source).startswith("/Volumes/") or not str(dest).startswith("/Volumes/"):
        raise ValueError("Existing-archive staging is restricted to owned UC volumes")
    if "umig6-p6269-2024" not in str(dest):
        raise ValueError("Output must be an isolated P6269 staging directory")
    if source.stat().st_size > 1_600_000_000:
        raise ValueError("Original compressed file exceeds pinned limit")
    h = hashlib.sha256()
    with source.open("rb") as stream:
        while True:
            chunk = stream.read(2 * 1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    if h.hexdigest() not in EXPECTED_ARCHIVES:
        raise ValueError("Original archive checksum mismatch after managed upload")
    dest.mkdir(parents=True, exist_ok=True)
    created = []
    files = []
    try:
        with zipfile.ZipFile(source) as archive:
            members = sorted((x for x in archive.infolist()
                              if x.filename.lower().endswith(".csv")),
                             key=lambda x: x.filename)
            if len(members) != 4:
                raise ValueError("Expected four original CSVs per source ZIP")
            for index, part in enumerate(members):
                if not re.fullmatch(r"[^/\\]+\.csv", part.filename, flags=re.IGNORECASE):
                    raise ValueError("Unexpected nested ZIP member path")
                if part.file_size > 1_000_000_000:
                    raise ValueError("CSV member exceeds safety cap")
                path = dest / ("source-%d-category-%d.csv" % (archive_index, index))
                if path.exists():
                    raise FileExistsError("Verified staged category already exists")
                temp = dest / (path.name + ".partial")
                try:
                    with archive.open(part) as reader, temp.open("xb") as writer:
                        count, checksum = _stream_copy(reader, writer, limit=1_000_000_000)
                    if count != part.file_size:
                        raise ValueError("Extracted original member byte count mismatch")
                    os.replace(temp, path)
                    created.append(path)
                finally:
                    if temp.exists():
                        temp.unlink()
                files.append({"archive_index": archive_index, "member_index": index,
                              "filename": path.name, "bytes": count, "sha256": checksum})
    except Exception:
        # Roll back only files created by this call; never touch other staged archives.
        for path in created:
            path.unlink(missing_ok=True)
        raise
    return {"format": "umig6-p6269-verified-upload-extraction-v1",
            "verified_original_archive_sha256": h.hexdigest(),
            "archive_index": archive_index,
            "source_members": files, "staged_csv_count": len(files),
            "paths": [str(dest / f["filename"]) for f in files],
            "utc_instant_mapping_verified": False}
