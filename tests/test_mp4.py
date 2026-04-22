from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from uploaderbot.mp4 import prepare_video_file, rewrite_faststart


class Mp4Tests(unittest.TestCase):
    def test_rewrite_faststart_moves_moov_before_mdat(self) -> None:
        blob = build_sample_mp4(moov_after_mdat=True)

        rewritten = rewrite_faststart(blob)

        atom_types = [rewritten[offset + 4 : offset + 8].decode("latin-1") for offset in top_level_offsets(rewritten)]
        self.assertEqual(atom_types[:3], ["ftyp", "moov", "mdat"])

        stco_offset = read_first_stco_offset(rewritten)
        mdat_data_offset = find_mdat_data_offset(rewritten)
        self.assertEqual(stco_offset, mdat_data_offset)

    def test_prepare_video_file_extracts_metadata_and_enables_streaming(self) -> None:
        blob = build_sample_mp4(moov_after_mdat=True)

        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "sample.mp4"
            path.write_bytes(blob)

            attributes = prepare_video_file(path)
            updated_blob = path.read_bytes()

        atom_types = [updated_blob[offset + 4 : offset + 8].decode("latin-1") for offset in top_level_offsets(updated_blob)]
        self.assertEqual(atom_types[:3], ["ftyp", "moov", "mdat"])
        self.assertEqual(attributes.duration_seconds, 5)
        self.assertEqual(attributes.width, 320)
        self.assertEqual(attributes.height, 180)
        self.assertTrue(attributes.supports_streaming)


def build_sample_mp4(*, moov_after_mdat: bool) -> bytes:
    ftyp = box("ftyp", b"isom" + (0).to_bytes(4, "big") + b"isom")
    mdat = box("mdat", b"12345678")

    original_mdat_data_offset = len(ftyp) + 8
    stco = build_stco([original_mdat_data_offset])
    stbl = box("stbl", stco)
    minf = box("minf", stbl)
    mdhd = build_mdhd(timescale=1000, duration=5000)
    hdlr = build_hdlr("vide")
    mdia = box("mdia", mdhd + hdlr + minf)
    tkhd = build_tkhd(width=320, height=180)
    trak = box("trak", tkhd + mdia)
    moov = box("moov", trak)

    if moov_after_mdat:
        return ftyp + mdat + moov
    return ftyp + moov + mdat


def box(box_type: str, payload: bytes) -> bytes:
    size = 8 + len(payload)
    return size.to_bytes(4, "big") + box_type.encode("latin-1") + payload


def build_stco(offsets: list[int]) -> bytes:
    payload = bytearray()
    payload.extend(b"\x00\x00\x00\x00")
    payload.extend(len(offsets).to_bytes(4, "big"))
    for offset in offsets:
        payload.extend(offset.to_bytes(4, "big"))
    return box("stco", bytes(payload))


def build_mdhd(*, timescale: int, duration: int) -> bytes:
    payload = bytearray()
    payload.extend(b"\x00\x00\x00\x00")
    payload.extend((0).to_bytes(4, "big"))
    payload.extend((0).to_bytes(4, "big"))
    payload.extend(timescale.to_bytes(4, "big"))
    payload.extend(duration.to_bytes(4, "big"))
    payload.extend((0).to_bytes(2, "big"))
    payload.extend((0).to_bytes(2, "big"))
    return box("mdhd", bytes(payload))


def build_hdlr(handler_type: str) -> bytes:
    payload = bytearray()
    payload.extend(b"\x00\x00\x00\x00")
    payload.extend((0).to_bytes(4, "big"))
    payload.extend(handler_type.encode("latin-1"))
    payload.extend((0).to_bytes(12, "big"))
    payload.extend(b"VideoHandler\x00")
    return box("hdlr", bytes(payload))


def build_tkhd(*, width: int, height: int) -> bytes:
    payload = bytearray()
    payload.extend((7).to_bytes(4, "big"))
    payload.extend((0).to_bytes(4, "big"))
    payload.extend((0).to_bytes(4, "big"))
    payload.extend((1).to_bytes(4, "big"))
    payload.extend((0).to_bytes(4, "big"))
    payload.extend((5000).to_bytes(4, "big"))
    payload.extend((0).to_bytes(8, "big"))
    payload.extend((0).to_bytes(2, "big"))
    payload.extend((0).to_bytes(2, "big"))
    payload.extend((0).to_bytes(2, "big"))
    payload.extend((0).to_bytes(2, "big"))
    payload.extend((0).to_bytes(36, "big"))
    payload.extend((width << 16).to_bytes(4, "big"))
    payload.extend((height << 16).to_bytes(4, "big"))
    return box("tkhd", bytes(payload))


def top_level_offsets(blob: bytes) -> list[int]:
    offsets: list[int] = []
    cursor = 0
    while cursor + 8 <= len(blob):
        offsets.append(cursor)
        cursor += int.from_bytes(blob[cursor : cursor + 4], "big")
    return offsets


def find_mdat_data_offset(blob: bytes) -> int:
    for offset in top_level_offsets(blob):
        if blob[offset + 4 : offset + 8] == b"mdat":
            return offset + 8
    raise AssertionError("mdat atom not found")


def read_first_stco_offset(blob: bytes) -> int:
    marker = b"stco"
    atom_offset = blob.index(marker) - 4
    entry_count_offset = atom_offset + 12
    entry_count = int.from_bytes(blob[entry_count_offset : entry_count_offset + 4], "big")
    if entry_count < 1:
        raise AssertionError("stco has no entries")
    return int.from_bytes(blob[entry_count_offset + 4 : entry_count_offset + 8], "big")


if __name__ == "__main__":
    unittest.main()
