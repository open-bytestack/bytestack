"""S3-backed writer for the bytestack storage format.

Buffers all data in memory and uploads the three stack files (``.data``,
``.idx``, ``.meta``) to S3 on :meth:`S3Writer.close`.

Usage::

    w = S3Writer.open("s3://my-bucket/my-prefix", controller="http://localhost:8080")
    index_id = w.put(b"hello", "greeting.txt")
    w.close()

Credentials are resolved via the standard boto3 credential chain
(environment variables ``AWS_ACCESS_KEY_ID``, ``AWS_SECRET_ACCESS_KEY``,
``AWS_REGION``, ``~/.aws/config``, IAM roles, etc.).

A custom S3-compatible endpoint (MinIO, gofakes3, …) can be configured
via the ``BYTESTACK_S3_ENDPOINT`` environment variable.
"""

from __future__ import annotations

import io
import json
import os
import secrets
import struct
import time
from typing import Optional

from ._crc32c import crc32c
from .error import ControllerError, Error, StackClosed, StackFull

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
# S3Writer
# ---------------------------------------------------------------------------

class S3Writer:
    """A sequential writer that produces one bytestack on S3-compatible storage.

    **Lifecycle:**:

        w = S3Writer.open("s3://bucket/prefix",      # controller mode (required)
                          controller="http://...")
        id1 = w.put(b"data", "f1.txt")
        id2 = w.put(b"more", "f2.txt", extra_meta=b'{"k":"v"}')
        w.close()
    """

    def __init__(self) -> None:
        self._bucket: str = ""
        self._prefix: str = ""
        self._controller: str = ""
        self._file_stack_id: int = 0
        self._header_stack_id: int = 0
        self._data_buf: Optional[io.BytesIO] = None
        self._idx_buf: Optional[io.BytesIO] = None
        self._meta_buf: Optional[io.BytesIO] = None
        self._data_offset: int = DATA_HEADER_SIZE
        self._meta_offset: int = 0
        self._total_raw_bytes: int = 0
        self._max_data_bytes: int = MAX_DATA_BYTES
        self._s3_client = None

    # -- open ----------------------------------------------------------------

    @classmethod
    def open(
        cls,
        location: str,
        *,
        controller: str,
    ) -> S3Writer:
        """Open a bytestack on S3-compatible storage.

        Parameters
        ----------
        location:
            ``s3://bucket/prefix`` — the bucket and optional key prefix where
            the ``.data`` / ``.idx`` / ``.meta`` objects will be stored.
        controller:
            The gRPC address of the Controller service
            (e.g. ``"http://localhost:8080"``).  **Required** for S3 mode.
        """
        if not location.startswith("s3://"):
            raise ValueError(
                f"S3Writer requires an s3:// location, got {location!r}"
            )

        s3_path = location[5:]  # strip "s3://"
        parts = s3_path.split("/", 1)
        bucket = parts[0]
        prefix = parts[1] if len(parts) > 1 else ""

        w = cls()
        w._bucket = bucket
        w._prefix = prefix
        w._controller = controller
        w._max_data_bytes = MAX_DATA_BYTES
        # Create S3 client (lazy — actual network use is in close()).
        w._s3_client = _create_s3_client()
        w._open_stack()
        return w

    def _open_stack(self) -> None:
        sid = _next_stack_id(self._controller)
        self._file_stack_id = sid
        self._header_stack_id = sid
        self._data_offset = DATA_HEADER_SIZE
        self._meta_offset = 0
        self._total_raw_bytes = 0
        self._data_buf = io.BytesIO()
        self._idx_buf = io.BytesIO()
        self._meta_buf = io.BytesIO()

        dh = struct.pack(_DATA_MAGIC_FMT, DATA_MAGIC, self._header_stack_id)
        assert len(dh) == 16
        self._data_buf.write(dh)
        self._data_buf.write(b"\x00" * (DATA_HEADER_SIZE - 16))

        ih = struct.pack(_INDEX_MAGIC_FMT, INDEX_MAGIC, self._header_stack_id)
        assert len(ih) == 16
        self._idx_buf.write(ih)

        mh = json.dumps(
            {"meta_magic_number": META_MAGIC, "stack_id": self._header_stack_id},
            separators=(",", ":"),
        )
        mh_line = (mh + "\n").encode("utf-8")
        self._meta_buf.write(mh_line)
        self._meta_offset = len(mh_line)

    def _upload_current_stack(self) -> None:
        if self._s3_client is None:
            return

        sid = self._file_stack_id
        for suffix, buf in (("data", self._data_buf), ("meta", self._meta_buf), ("idx", self._idx_buf)):
            if buf is None:
                continue
            key = _format_path(sid, suffix)
            if self._prefix:
                key = self._prefix + "/" + key
            buf.seek(0)
            self._s3_client.put_object(Bucket=self._bucket, Key=key, Body=buf)
            buf.close()

        self._data_buf = None
        self._idx_buf = None
        self._meta_buf = None

    def _rotate_stack(self) -> None:
        self._upload_current_stack()
        self._open_stack()

    # -- properties ----------------------------------------------------------

    @property
    def stack_id(self) -> int:
        """The stack identifier used for file naming."""
        return self._file_stack_id

    @property
    def header_stack_id(self) -> int:
        """The stack identifier written into binary headers."""
        return self._header_stack_id

    @property
    def location(self) -> str:
        """The ``s3://bucket/prefix`` location string."""
        path = self._bucket
        if self._prefix:
            path += "/" + self._prefix
        return f"s3://{path}"

    @property
    def max_data_bytes(self) -> int:
        return self._max_data_bytes

    @property
    def total_raw_bytes(self) -> int:
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
        data_buf = self._data_buf
        idx_buf = self._idx_buf
        meta_buf = self._meta_buf
        if data_buf is None or idx_buf is None or meta_buf is None:
            raise StackClosed()

        # --- Size guard -----------------------------------------------------
        if len(data) > self._max_data_bytes:
            raise StackFull(len(data), self._max_data_bytes)

        new_total = self._total_raw_bytes + len(data)
        if new_total > self._max_data_bytes:
            self._rotate_stack()
            data_buf = self._data_buf
            idx_buf = self._idx_buf
            meta_buf = self._meta_buf
            if data_buf is None or idx_buf is None or meta_buf is None:
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

        # Pre-compute index_id.
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

        # --- Write order: data → meta → idx -------------------------------
        data_buf.write(dr_header)
        data_buf.write(data)
        if padding_size:
            data_buf.write(b"\x00" * padding_size)
        self._data_offset += raw_bytes + padding_size

        meta_buf.write(mr_line)
        self._meta_offset += mr_line_len

        idx_buf.write(ir_bytes)

        self._total_raw_bytes += len(data)
        return index_id

    # -- close ---------------------------------------------------------------

    def close(self) -> None:
        """Upload the three stack files to S3 and clean up.

        After this call any further :meth:`put` raises :class:`StackClosed`.
        Calling ``close()`` multiple times is safe.
        """
        if self._s3_client is None:
            return

        self._upload_current_stack()
        self._s3_client = None


# ---------------------------------------------------------------------------
# S3 client factory
# ---------------------------------------------------------------------------

def _create_s3_client():
    """Create a boto3 S3 client using environment credentials.

    Respects ``BYTESTACK_S3_ENDPOINT`` for MinIO-compatible stores.
    """
    import boto3

    endpoint = os.environ.get("BYTESTACK_S3_ENDPOINT")
    kwargs = {}
    if endpoint:
        kwargs["endpoint_url"] = endpoint
        # Path-style addressing for MinIO/compatible stores.
        from botocore.config import Config as BotoConfig
        kwargs["config"] = BotoConfig(s3={"addressing_style": "path"})

    return boto3.client("s3", **kwargs)


# ---------------------------------------------------------------------------
# Controller gRPC helper
# ---------------------------------------------------------------------------

def _next_stack_id(addr: str) -> int:
    """Obtain a fresh ``stack_id`` from the Controller gRPC service."""
    try:
        import sys as _sys

        _base = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(
            os.path.abspath(__file__),
        ))))
        _proto_dir = os.path.join(_base, "proto", "src", "controller")
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
