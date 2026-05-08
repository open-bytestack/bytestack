from __future__ import annotations

from pathlib import Path

from bytestack_sdk import LocalWriter, open_writer
from bytestack_sdk._crc32c import crc32c


def test_crc32c_standard_vectors():
    assert crc32c(b"") == 0x00000000
    assert crc32c(b"123456789") == 0xE3069283


def test_open_writer_uses_local_writer(tmp_path: Path):
    writer = open_writer(tmp_path)
    assert isinstance(writer, LocalWriter)
    writer.close()


def test_local_writer_rotates_when_full(tmp_path: Path):
    writer = LocalWriter.open(tmp_path)
    writer._max_data_bytes = 5

    first_stack_id = writer.stack_id
    first_id = writer.put(b"abcd", "one.txt")
    second_id = writer.put(b"efgh", "two.txt")

    assert first_id.startswith(f"{first_stack_id},")
    assert second_id != first_id
    assert writer.stack_id != first_stack_id

    writer.close()

    data_files = sorted(tmp_path.glob("*.data"))
    assert len(data_files) == 2
