from __future__ import annotations

import json
import math
import struct
import subprocess
import textwrap
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from bytestack_sdk import LocalWriter
from bytestack_sdk.error import StackClosed
from bytestack_sdk._crc32c import crc32c


ALIGNMENT = 4096
DATA_MAGIC = 47494638
INDEX_MAGIC = 5201314
META_MAGIC = 1314920
RECORD_MAGIC_START = 257758
RECORD_MAGIC_END = 857752
DATA_HEADER_SIZE = 4096
DATA_RECORD_HEADER_SIZE = 20
INDEX_HEADER_SIZE = 16
INDEX_RECORD_SIZE = 28
LOCAL_STACK_ID = 0xFFFF_FFFF_FFFF_FFFF

META_HEADER_KEYS = ["meta_magic_number", "stack_id"]
META_RECORD_KEYS = [
    "create_time",
    "offset_data",
    "size_data",
    "cookie",
    "filename",
    "extra",
]


@dataclass(frozen=True)
class RecordFixture:
    data: bytes
    filename: str
    extra: bytes | None


FIXTURES = [
    RecordFixture(b"", "empty.bin", None),
    RecordFixture(b"hello world", "hello.txt", None),
    RecordFixture(
        bytes(i % 256 for i in range(4076)),
        "aligned.bin",
        bytes([0, 1, 2, 250, 255]),
    ),
    RecordFixture(bytes(i % 256 for i in range(6000)), "unicode-λ.txt", b'{"k":"v"}'),
]


def repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def run_python_writer(out_dir: Path) -> dict[str, list[str] | str]:
    writer = LocalWriter.open(out_dir)
    ids = []
    for item in FIXTURES:
        ids.append(writer.put(item.data, item.filename, item.extra))

    result = {
        "stack_id": str(writer.stack_id),
        "header_stack_id": str(writer.header_stack_id),
        "total_raw_bytes": str(writer.total_raw_bytes),
        "id": ids,
    }
    writer.close()

    try:
        writer.put(b"late", "late.txt")
    except StackClosed:
        result["put_after_close"] = "True"
    else:
        result["put_after_close"] = "False"

    return result


def parse_key_value_output(output: str) -> dict[str, list[str] | str]:
    result: dict[str, list[str] | str] = {"id": []}
    for line in output.splitlines():
        key, value = line.split("=", 1)
        if key == "id":
            assert isinstance(result["id"], list)
            result["id"].append(value)
        else:
            result[key] = value
    return result


def run_checked(args: list[str], cwd: Path) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(
        args,
        cwd=cwd,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    assert completed.returncode == 0, (
        f"command failed: {' '.join(args)}\n"
        f"stdout:\n{completed.stdout}\n"
        f"stderr:\n{completed.stderr}"
    )
    return completed


def run_go_writer(out_dir: Path, tmp_path: Path) -> dict[str, list[str] | str]:
    source = tmp_path / "cross_sdk_writer.go"
    source.write_text(
        textwrap.dedent(
            """
            package main

            import (
                "fmt"
                "os"

                "github.com/open-bytestack/bytestack/sdk/golang/bytestack"
            )

            func seqBytes(n int) []byte {
                out := make([]byte, n)
                for i := range out {
                    out[i] = byte(i % 256)
                }
                return out
            }

            func must[T any](value T, err error) T {
                if err != nil {
                    panic(err)
                }
                return value
            }

            func main() {
                w := must(bytestack.Open(os.Args[1]))
                ids := []string{
                    must(w.Put([]byte{}, "empty.bin", nil)),
                    must(w.Put([]byte("hello world"), "hello.txt", nil)),
                    must(w.Put(seqBytes(4076), "aligned.bin", []byte{0, 1, 2, 250, 255})),
                    must(w.Put(seqBytes(6000), "unicode-λ.txt", []byte(`{"k":"v"}`))),
                }

                fmt.Printf("stack_id=%d\\n", w.StackID())
                fmt.Printf("header_stack_id=%d\\n", w.HeaderStackID())
                fmt.Printf("total_raw_bytes=%d\\n", w.TotalRawBytes())

                if err := w.Close(); err != nil {
                    panic(err)
                }

                _, err := w.Put([]byte("late"), "late.txt", nil)
                fmt.Printf("put_after_close=%t\\n", err == bytestack.ErrStackClosed)
                for _, id := range ids {
                    fmt.Printf("id=%s\\n", id)
                }
            }
            """
        ),
        encoding="utf-8",
    )

    completed = run_checked(
        ["go", "run", str(source), str(out_dir)],
        cwd=repo_root(),
    )
    return parse_key_value_output(completed.stdout)


def run_rust_writer(out_dir: Path) -> dict[str, list[str] | str]:
    completed = run_checked(
        [
            "cargo",
            "run",
            "--quiet",
            "-p",
            "bytestack-sdk",
            "--example",
            "cross_sdk_writer",
            "--",
            str(out_dir),
        ],
        cwd=repo_root(),
    )
    return parse_key_value_output(completed.stdout)


def expected_record_size(payload_size: int) -> int:
    raw_size = DATA_RECORD_HEADER_SIZE + payload_size
    return int(math.ceil(raw_size / ALIGNMENT) * ALIGNMENT)


def read_one_stack_file(out_dir: Path, suffix: str, stack_id: int) -> bytes:
    path = out_dir / f"0x{stack_id:04x}.{suffix}"
    assert path.exists(), f"missing {path}"
    return path.read_bytes()


def parse_and_validate_stack(
    out_dir: Path,
    run_result: dict[str, list[str] | str],
) -> list[dict[str, int | str | list[int]]]:
    stack_id = int(run_result["stack_id"])
    assert int(run_result["header_stack_id"]) == LOCAL_STACK_ID
    assert int(run_result["total_raw_bytes"]) == sum(len(item.data) for item in FIXTURES)
    assert run_result["put_after_close"].lower() == "true"
    assert sorted(p.suffix for p in out_dir.iterdir()) == [".data", ".idx", ".meta"]

    data_bytes = read_one_stack_file(out_dir, "data", stack_id)
    idx_bytes = read_one_stack_file(out_dir, "idx", stack_id)
    meta_bytes = read_one_stack_file(out_dir, "meta", stack_id)

    data_magic, data_stack_id = struct.unpack_from("<QQ", data_bytes, 0)
    assert data_magic == DATA_MAGIC
    assert data_stack_id == LOCAL_STACK_ID
    assert data_bytes[16:DATA_HEADER_SIZE] == b"\x00" * (DATA_HEADER_SIZE - 16)

    idx_magic, idx_stack_id = struct.unpack_from("<QQ", idx_bytes, 0)
    assert idx_magic == INDEX_MAGIC
    assert idx_stack_id == LOCAL_STACK_ID
    assert len(idx_bytes) == INDEX_HEADER_SIZE + len(FIXTURES) * INDEX_RECORD_SIZE

    meta_lines = meta_bytes.splitlines(keepends=True)
    assert len(meta_lines) == len(FIXTURES) + 1
    meta_header = json.loads(meta_lines[0])
    assert list(meta_header) == META_HEADER_KEYS
    assert meta_header == {"meta_magic_number": META_MAGIC, "stack_id": LOCAL_STACK_ID}

    ids = run_result["id"]
    assert isinstance(ids, list)
    assert len(ids) == len(FIXTURES)

    data_offset = DATA_HEADER_SIZE
    meta_offset = len(meta_lines[0])
    normalized = []
    for index, fixture in enumerate(FIXTURES):
        idx_offset = INDEX_HEADER_SIZE + index * INDEX_RECORD_SIZE
        cookie, offset_data, size_data, offset_meta, size_meta = struct.unpack_from(
            "<IQIQI", idx_bytes, idx_offset
        )
        assert offset_data == data_offset
        assert size_data == len(fixture.data)
        assert offset_meta == meta_offset
        assert size_meta == len(meta_lines[index + 1])
        assert ids[index] == f"{stack_id},{offset_data:x}{cookie:08x}"

        start_magic, data_cookie, data_size, data_crc, end_magic = struct.unpack_from(
            "<IIIII", data_bytes, data_offset
        )
        assert start_magic == RECORD_MAGIC_START
        assert data_cookie == cookie
        assert data_size == len(fixture.data)
        assert data_crc == crc32c(fixture.data)
        assert end_magic == RECORD_MAGIC_END

        payload_start = data_offset + DATA_RECORD_HEADER_SIZE
        payload_end = payload_start + len(fixture.data)
        assert data_bytes[payload_start:payload_end] == fixture.data

        record_size = expected_record_size(len(fixture.data))
        padding = data_bytes[payload_end : data_offset + record_size]
        assert padding == b"\x00" * len(padding)

        meta_record = json.loads(meta_lines[index + 1])
        assert list(meta_record) == META_RECORD_KEYS
        assert isinstance(meta_record["create_time"], int)
        assert meta_record["offset_data"] == offset_data
        assert meta_record["size_data"] == len(fixture.data)
        assert meta_record["cookie"] == cookie
        assert meta_record["filename"] == fixture.filename
        assert meta_record["extra"] == list(fixture.extra or b"")
        assert b": " not in meta_lines[index + 1]
        assert b", " not in meta_lines[index + 1]

        normalized.append(
            {
                "offset_data": offset_data,
                "size_data": size_data,
                "filename": meta_record["filename"],
                "extra": meta_record["extra"],
                "record_size": record_size,
            }
        )
        data_offset += record_size
        meta_offset += len(meta_lines[index + 1])

    assert len(data_bytes) == data_offset
    return normalized


def test_local_writer_format_is_consistent_across_sdks(tmp_path: Path):
    runners: dict[str, Callable[[Path], dict[str, list[str] | str]]] = {
        "python": run_python_writer,
        "go": lambda out_dir: run_go_writer(out_dir, tmp_path),
        "rust": run_rust_writer,
    }

    normalized_by_language = {}
    for language, runner in runners.items():
        out_dir = tmp_path / language
        out_dir.mkdir()
        run_result = runner(out_dir)
        normalized_by_language[language] = parse_and_validate_stack(out_dir, run_result)

    assert normalized_by_language["go"] == normalized_by_language["python"]
    assert normalized_by_language["rust"] == normalized_by_language["python"]
