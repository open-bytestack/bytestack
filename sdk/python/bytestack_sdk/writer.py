"""Local directory writer for the bytestack storage format.

Usage::

    # Local mode (timestamp-based naming, u64::MAX in headers):
    w = LocalWriter.open("/tmp/mystack")
    index_id = w.put(b"hello", "greeting.txt")
    w.close()

    # Controller mode (real stack_id from gRPC):
    w = LocalWriter.open("/tmp/mystack", controller="http://localhost:8080")
    index_id = w.put(b"hello", "greeting.txt")
    w.close()
"""

from __future__ import annotations

import json
import os
import secrets
import struct
import time
from pathlib import Path
from typing import BinaryIO, Optional

from ._crc32c import crc32c
from .error import ControllerError, StackClosed, StackFull

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

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
MAX_DATA_BYTES = 5 * 1024 * 1024 * 1024  # 5 GiB

# Struct format strings (little-endian, no padding).
_DATA_MAGIC_FMT = "<QQ"
_INDEX_MAGIC_FMT = "<QQ"
_DATA_RECORD_HDR_FMT = "<IIIII"
_INDEX_RECORD_FMT = "<IQIQI"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _format_path(stack_id: int, suffix: str) -> str:
    """Return the file name for a stack file, e.g. ``0x0064.data``."""
    return f"0x{stack_id:04x}.{suffix}"


# ---------------------------------------------------------------------------
# LocalWriter
# ---------------------------------------------------------------------------

class LocalWriter:
    """A sequential writer that produces one bytestack on the local filesystem.

    **Lifecycle:**:

        w = LocalWriter.open("/tmp/mystack")          # local mode
        w = LocalWriter.open("/tmp/mystack",           # controller mode
                             controller="http://...")
        id1 = w.put(b"data", "f1.txt")
        id2 = w.put(b"more", "f2.txt", extra_meta=b'{"k":"v"}')
        w.close()
        # w.put(...)  → raises StackClosed
    """

    def __init__(self) -> None:
        # Set by open().
        self._dir: str = ""
        self._controller: Optional[str] = None
        self._file_stack_id: int = 0
        self._header_stack_id: int = 0
        self._data_file: Optional[BinaryIO] = None
        self._idx_file: Optional[BinaryIO] = None
        self._meta_file: Optional[BinaryIO] = None
        self._data_offset: int = DATA_HEADER_SIZE
        self._meta_offset: int = 0
        self._total_raw_bytes: int = 0
        self._max_data_bytes: int = MAX_DATA_BYTES

    # -- open ----------------------------------------------------------------

    @classmethod
    def open(
        cls,
        path: str | os.PathLike[str],
        *,
        controller: Optional[str] = None,
    ) -> LocalWriter:
        """Open (or create) a stack inside *path*.

        Parameters
        ----------
        path:
            Local directory that will hold the ``.data`` / ``.idx`` / ``.meta``
            files.
        controller:
            ``None`` for local mode (timestamp + ``u64::MAX`` in headers).
            A gRPC address such as ``"http://localhost:8080"`` to obtain a real
            ``stack_id`` from the Controller service.
        """
        w = cls()
        w._dir = str(path)
        w._controller = controller
        os.makedirs(w._dir, exist_ok=True)
        w._max_data_bytes = MAX_DATA_BYTES
        w._open_stack()
        return w

    def _allocate_stack_ids(self) -> tuple[int, int]:
        if self._controller is not None:
            sid = _next_stack_id(self._controller)
            return sid, sid

        sid = int(time.time())
        while os.path.exists(os.path.join(self._dir, _format_path(sid, "data"))):
            sid += 1
        return sid, LOCAL_STACK_ID

    def _open_stack(self) -> None:
        self._file_stack_id, self._header_stack_id = self._allocate_stack_ids()
        self._data_offset = DATA_HEADER_SIZE
        self._total_raw_bytes = 0

        data_path = os.path.join(self._dir, _format_path(self._file_stack_id, "data"))
        idx_path = os.path.join(self._dir, _format_path(self._file_stack_id, "idx"))
        meta_path = os.path.join(self._dir, _format_path(self._file_stack_id, "meta"))

        self._data_file = open(data_path, "xb")
        self._idx_file = open(idx_path, "xb")
        self._meta_file = open(meta_path, "xb")

        dh = struct.pack(_DATA_MAGIC_FMT, DATA_MAGIC, self._header_stack_id)
        assert len(dh) == 16
        self._data_file.write(dh)
        self._data_file.write(b"\x00" * (DATA_HEADER_SIZE - 16))

        ih = struct.pack(_INDEX_MAGIC_FMT, INDEX_MAGIC, self._header_stack_id)
        assert len(ih) == 16
        self._idx_file.write(ih)

        mh = json.dumps(
            {"meta_magic_number": META_MAGIC, "stack_id": self._header_stack_id},
            separators=(",", ":"),
        )
        mh_line = (mh + "\n").encode("utf-8")
        self._meta_file.write(mh_line)
        self._meta_offset = len(mh_line)

    def _rotate_stack(self) -> None:
        self.close()
        self._open_stack()

    # -- properties ----------------------------------------------------------

    @property
    def stack_id(self) -> int:
        """The stack identifier used for file naming (``0x{stack_id}.{suffix}``).

        In local mode this is a Unix timestamp; in controller mode it is the
        real ``stack_id`` returned by the Controller.
        """
        return self._file_stack_id

    @property
    def header_stack_id(self) -> int:
        """The stack identifier written into binary headers.

        In local mode this is ``u64::MAX``; in controller mode it equals
        :attr:`stack_id`.
        """
        return self._header_stack_id

    @property
    def dir(self) -> str:
        """The local directory path this writer is writing into."""
        return self._dir

    @property
    def max_data_bytes(self) -> int:
        """The maximum raw payload bytes allowed before rotation is needed."""
        return self._max_data_bytes

    @property
    def total_raw_bytes(self) -> int:
        """The raw payload bytes written so far (excluding headers/padding)."""
        return self._total_raw_bytes

    # -- put -----------------------------------------------------------------

    def put(
        self,
        data: bytes,
        filename: str,
        extra_meta: Optional[bytes] = None,
    ) -> str:
        """Write one record into the stack.

        Parameters
        ----------
        data:
            Raw payload bytes.
        filename:
            Original file name.
        extra_meta:
            Arbitrary application metadata blob (may be empty).

        Returns
        -------
        index_id:
            A globally unique string of the form
            ``{stack_id},{offset_data:x}{cookie:08x}``.
        """
        data_f = self._data_file
        idx_f = self._idx_file
        meta_f = self._meta_file
        if data_f is None or idx_f is None or meta_f is None:
            raise StackClosed()

        # --- Size guard -----------------------------------------------------
        if len(data) > self._max_data_bytes:
            raise StackFull(len(data), self._max_data_bytes)

        new_total = self._total_raw_bytes + len(data)
        if new_total > self._max_data_bytes:
            self._rotate_stack()
            data_f = self._data_file
            idx_f = self._idx_file
            meta_f = self._meta_file
            if data_f is None or idx_f is None or meta_f is None:
                raise StackClosed()

        # --- Build records --------------------------------------------------
        cookie = secrets.randbits(32)
        crc = crc32c(data)

        data_offset = self._data_offset
        meta_offset = self._meta_offset
        create_time = int(time.time())

        # MetaRecord (JSON + newline).
        mr = {
            "create_time": create_time,
            "offset_data": data_offset,
            "size_data": len(data),
            "cookie": cookie,
            "filename": filename,
            "extra": list(extra_meta) if extra_meta else [],
        }
        mr_json = json.dumps(mr, separators=(",", ":"), ensure_ascii=False)
        mr_line = (mr_json + "\n").encode("utf-8")
        mr_line_len = len(mr_line)

        # IndexRecord (bincode-equivalent struct).
        ir_bytes = struct.pack(
            _INDEX_RECORD_FMT,
            cookie,
            data_offset,
            len(data),
            meta_offset,
            mr_line_len,
        )
        assert len(ir_bytes) == INDEX_RECORD_SIZE

        # Pre-compute index_id before the write advances offsets.
        index_id = f"{self._file_stack_id},{data_offset:x}{cookie:08x}"

        # DataRecord components.
        dr_header = struct.pack(
            _DATA_RECORD_HDR_FMT,
            RECORD_MAGIC_START,
            cookie,
            len(data),
            crc,
            RECORD_MAGIC_END,
        )
        assert len(dr_header) == DATA_RECORD_HEADER_SIZE
        raw_bytes = DATA_RECORD_HEADER_SIZE + len(data)
        padding_size = (ALIGNMENT - (raw_bytes % ALIGNMENT)) % ALIGNMENT

        # --- Write order: data → flush → meta → idx -----------------------
        data_f.write(dr_header)
        data_f.write(data)
        if padding_size:
            data_f.write(b"\x00" * padding_size)
        data_f.flush()
        os.fsync(data_f.fileno())
        self._data_offset += raw_bytes + padding_size

        meta_f.write(mr_line)
        meta_f.flush()
        os.fsync(meta_f.fileno())
        self._meta_offset += mr_line_len

        idx_f.write(ir_bytes)
        idx_f.flush()
        os.fsync(idx_f.fileno())

        self._total_raw_bytes += len(data)
        return index_id

    # -- close ---------------------------------------------------------------

    def close(self) -> None:
        """Flush and close the three stack files.

        After this call any further :meth:`put` raises
        :class:`~bytestack_sdk.error.StackClosed`.
        Calling ``close()`` multiple times is safe (subsequent calls are
        no-ops).
        """
        for f in (self._data_file, self._idx_file, self._meta_file):
            if f is not None:
                f.flush()
                os.fsync(f.fileno())
                f.close()

        self._data_file = None
        self._idx_file = None
        self._meta_file = None


# ---------------------------------------------------------------------------
# Controller gRPC helper
# ---------------------------------------------------------------------------

def _next_stack_id(addr: str) -> int:
    """Obtain a fresh ``stack_id`` from the Controller gRPC service at *addr*."""
    try:
        import sys as _sys

        # Locate the generated proto files.
        _base = Path(__file__).resolve().parent.parent.parent.parent
        _proto_dir = str(_base / "proto" / "src" / "controller")
        if _proto_dir not in _sys.path:
            _sys.path.insert(0, _proto_dir)

        import grpc
        from controller_pb2_grpc import ControllerStub
        from google.protobuf.empty_pb2 import Empty

        channel = grpc.insecure_channel(addr)
        stub = ControllerStub(channel)
        resp = stub.NextStackID(Empty())
        channel.close()
        return resp.stack_id

    except ImportError as exc:
        raise ControllerError(
            f"cannot import controller proto stubs: {exc}"
        ) from exc
    except Exception as exc:
        raise ControllerError(str(exc)) from exc
