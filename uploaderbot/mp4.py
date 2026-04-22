from __future__ import annotations

import math
import importlib
import logging
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path


STREAMABLE_MP4_SUFFIXES = {".m4v", ".mov", ".mp4"}
CONTAINER_BOX_TYPES = {
    "dinf",
    "edts",
    "mdia",
    "minf",
    "moov",
    "stbl",
    "trak",
    "udta",
}
THUMBNAIL_SUFFIX = ".telegram-thumb.jpg"
THUMBNAIL_MAX_BYTES = 200 * 1024
THUMBNAIL_QUALITY_STEPS = (4, 8, 12, 18, 24)


logger = logging.getLogger("uploaderbot")


class Mp4ProcessingError(ValueError):
    pass


@dataclass(slots=True)
class Atom:
    type: str
    start: int
    size: int
    header_size: int

    @property
    def data_start(self) -> int:
        return self.start + self.header_size

    @property
    def end(self) -> int:
        return self.start + self.size


@dataclass(slots=True)
class VideoAttributes:
    duration_seconds: int | None = None
    width: int | None = None
    height: int | None = None
    supports_streaming: bool = False
    thumbnail_path: Path | None = None


def prepare_video_file(path: Path) -> VideoAttributes:
    if path.suffix.lower() not in STREAMABLE_MP4_SUFFIXES:
        return VideoAttributes()

    blob = path.read_bytes()
    top_level_atoms = list(iter_atoms(blob))
    moov_atom = _find_first_atom(top_level_atoms, "moov")
    mdat_atom = _find_first_atom(top_level_atoms, "mdat")
    if moov_atom is None or mdat_atom is None:
        return VideoAttributes()

    if moov_atom.start > mdat_atom.start:
        blob = rewrite_faststart(blob, top_level_atoms)
        path.write_bytes(blob)
        top_level_atoms = list(iter_atoms(blob))
        moov_atom = _find_first_atom(top_level_atoms, "moov")
        if moov_atom is None:
            raise Mp4ProcessingError("Rewritten MP4 is missing moov atom")

    attributes = extract_video_attributes(blob, moov_atom)
    attributes.thumbnail_path = build_video_thumbnail(path, attributes.duration_seconds)
    return attributes


def rewrite_faststart(blob: bytes, top_level_atoms: list[Atom] | None = None) -> bytes:
    atoms = top_level_atoms or list(iter_atoms(blob))
    moov_atom = _find_first_atom(atoms, "moov")
    mdat_atom = _find_first_atom(atoms, "mdat")
    if moov_atom is None or mdat_atom is None or moov_atom.start < mdat_atom.start:
        return blob

    patched_moov = patch_moov_chunk_offsets(blob[moov_atom.start : moov_atom.end], moov_atom.size)
    reordered_parts: list[bytes] = []
    for atom in atoms:
        if atom.type == "moov":
            continue
        if atom.start == mdat_atom.start:
            reordered_parts.append(patched_moov)
        reordered_parts.append(blob[atom.start : atom.end])
    return b"".join(reordered_parts)


def extract_video_attributes(blob: bytes, moov_atom: Atom) -> VideoAttributes:
    video_track = _find_video_track(blob, moov_atom)
    if video_track is None:
        return VideoAttributes(supports_streaming=True)

    tkhd_atom = _find_child_atom(blob, video_track, "tkhd")
    mdia_atom = _find_child_atom(blob, video_track, "mdia")
    mdhd_atom = _find_child_atom(blob, mdia_atom, "mdhd") if mdia_atom else None

    width, height = read_tkhd_dimensions(blob, tkhd_atom) if tkhd_atom else (None, None)
    duration_seconds = read_mdhd_duration_seconds(blob, mdhd_atom) if mdhd_atom else None
    return VideoAttributes(
        duration_seconds=duration_seconds,
        width=width,
        height=height,
        supports_streaming=True,
    )


def patch_moov_chunk_offsets(moov_box: bytes, delta: int) -> bytes:
    patched = bytearray(moov_box)
    _patch_chunk_offsets_recursive(patched, 8, len(patched), delta)
    return bytes(patched)


def read_tkhd_dimensions(blob: bytes, tkhd_atom: Atom) -> tuple[int | None, int | None]:
    data = blob[tkhd_atom.data_start : tkhd_atom.end]
    if len(data) < 84:
        return None, None

    version = data[0]
    offset = 76 if version == 0 else 88
    if len(data) < offset + 8:
        return None, None

    width = int.from_bytes(data[offset : offset + 4], "big") >> 16
    height = int.from_bytes(data[offset + 4 : offset + 8], "big") >> 16
    return width or None, height or None


def read_mdhd_duration_seconds(blob: bytes, mdhd_atom: Atom) -> int | None:
    data = blob[mdhd_atom.data_start : mdhd_atom.end]
    if len(data) < 20:
        return None

    version = data[0]
    if version == 1:
        if len(data) < 32:
            return None
        timescale = int.from_bytes(data[20:24], "big")
        duration = int.from_bytes(data[24:32], "big")
    else:
        timescale = int.from_bytes(data[12:16], "big")
        duration = int.from_bytes(data[16:20], "big")

    if timescale <= 0 or duration <= 0:
        return None
    return max(1, math.ceil(duration / timescale))


def build_video_thumbnail(path: Path, duration_seconds: int | None) -> Path | None:
    ffmpeg_executable = resolve_ffmpeg_executable()
    if ffmpeg_executable is None:
        return None

    thumbnail_path = path.with_name(f"{path.stem}{THUMBNAIL_SUFFIX}")
    timestamp_seconds = thumbnail_timestamp_seconds(duration_seconds)
    last_error = "unknown ffmpeg error"

    for quality in THUMBNAIL_QUALITY_STEPS:
        thumbnail_path.unlink(missing_ok=True)
        command = [
            ffmpeg_executable,
            "-y",
            "-ss",
            f"{timestamp_seconds:.3f}",
            "-i",
            str(path),
            "-frames:v",
            "1",
            "-an",
            "-sn",
            "-dn",
            "-map_metadata",
            "-1",
            "-vf",
            "scale=320:320:force_original_aspect_ratio=decrease",
            "-pix_fmt",
            "yuvj420p",
            "-q:v",
            str(quality),
            str(thumbnail_path),
        ]
        result = subprocess.run(command, capture_output=True, text=True, check=False)
        if result.returncode != 0:
            last_error = result.stderr.strip() or result.stdout.strip() or "unknown ffmpeg error"
            continue

        if not thumbnail_path.exists() or thumbnail_path.stat().st_size == 0:
            last_error = "ffmpeg did not create a thumbnail file"
            continue

        if thumbnail_path.stat().st_size <= THUMBNAIL_MAX_BYTES:
            return thumbnail_path

    thumbnail_path.unlink(missing_ok=True)
    logger.warning("Could not create thumbnail for %s: %s", path, last_error)
    return None


def resolve_ffmpeg_executable() -> str | None:
    try:
        imageio_ffmpeg = importlib.import_module("imageio_ffmpeg")
    except ImportError:
        return shutil.which("ffmpeg")

    try:
        return imageio_ffmpeg.get_ffmpeg_exe()
    except Exception as exc:
        logger.warning("Could not resolve imageio ffmpeg binary: %s", exc)
        return shutil.which("ffmpeg")


def thumbnail_timestamp_seconds(duration_seconds: int | None) -> float:
    if duration_seconds is None:
        return 1.0
    if duration_seconds <= 1:
        return 0.0
    return min(max(duration_seconds * 0.2, 0.5), 3.0)


def _patch_chunk_offsets_recursive(blob: bytearray, start: int, end: int, delta: int) -> None:
    for atom in iter_atoms(blob, start, end):
        if atom.type == "stco":
            _patch_stco_atom(blob, atom, delta)
            continue
        if atom.type == "co64":
            _patch_co64_atom(blob, atom, delta)
            continue
        if atom.type in CONTAINER_BOX_TYPES:
            _patch_chunk_offsets_recursive(blob, atom.data_start, atom.end, delta)


def _patch_stco_atom(blob: bytearray, atom: Atom, delta: int) -> None:
    data_start = atom.data_start
    entry_count = int.from_bytes(blob[data_start + 4 : data_start + 8], "big")
    cursor = data_start + 8
    for _ in range(entry_count):
        updated = int.from_bytes(blob[cursor : cursor + 4], "big") + delta
        if updated > 0xFFFFFFFF:
            raise Mp4ProcessingError("Chunk offset exceeds 32-bit range after faststart rewrite")
        blob[cursor : cursor + 4] = updated.to_bytes(4, "big")
        cursor += 4


def _patch_co64_atom(blob: bytearray, atom: Atom, delta: int) -> None:
    data_start = atom.data_start
    entry_count = int.from_bytes(blob[data_start + 4 : data_start + 8], "big")
    cursor = data_start + 8
    for _ in range(entry_count):
        updated = int.from_bytes(blob[cursor : cursor + 8], "big") + delta
        blob[cursor : cursor + 8] = updated.to_bytes(8, "big")
        cursor += 8


def iter_atoms(blob: bytes | bytearray, start: int = 0, end: int | None = None):
    limit = len(blob) if end is None else end
    offset = start
    while offset + 8 <= limit:
        size = int.from_bytes(blob[offset : offset + 4], "big")
        atom_type = bytes(blob[offset + 4 : offset + 8]).decode("latin-1")
        header_size = 8
        if size == 1:
            if offset + 16 > limit:
                raise Mp4ProcessingError("Invalid MP4 atom with truncated extended size")
            size = int.from_bytes(blob[offset + 8 : offset + 16], "big")
            header_size = 16
        elif size == 0:
            size = limit - offset

        if size < header_size or offset + size > limit:
            raise Mp4ProcessingError(f"Invalid MP4 atom size for {atom_type}")

        atom = Atom(type=atom_type, start=offset, size=size, header_size=header_size)
        yield atom
        offset += size


def _find_first_atom(atoms: list[Atom], atom_type: str) -> Atom | None:
    for atom in atoms:
        if atom.type == atom_type:
            return atom
    return None


def _find_video_track(blob: bytes, moov_atom: Atom) -> Atom | None:
    for atom in iter_atoms(blob, moov_atom.data_start, moov_atom.end):
        if atom.type != "trak":
            continue
        mdia_atom = _find_child_atom(blob, atom, "mdia")
        if mdia_atom is None:
            continue
        hdlr_atom = _find_child_atom(blob, mdia_atom, "hdlr")
        if hdlr_atom is None:
            continue
        if _read_hdlr_handler_type(blob, hdlr_atom) == "vide":
            return atom
    return None


def _find_child_atom(blob: bytes, parent_atom: Atom | None, child_type: str) -> Atom | None:
    if parent_atom is None:
        return None
    for atom in iter_atoms(blob, parent_atom.data_start, parent_atom.end):
        if atom.type == child_type:
            return atom
    return None


def _read_hdlr_handler_type(blob: bytes, hdlr_atom: Atom) -> str | None:
    data = blob[hdlr_atom.data_start : hdlr_atom.end]
    if len(data) < 12:
        return None
    return data[8:12].decode("latin-1")
