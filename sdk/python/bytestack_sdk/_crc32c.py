"""Pure Python CRC-32C (Castagnoli) implementation.

Polynomial: 0x1EDC6F41 (CRC-32/ISCSI).
"""

from __future__ import annotations

_POLY = 0x82F63B78
_TABLE: list[int] | None = None


def _make_table() -> list[int]:
    table: list[int] = []
    for i in range(256):
        crc = i
        for _ in range(8):
            if crc & 1:
                crc = (crc >> 1) ^ _POLY
            else:
                crc >>= 1
        table.append(crc)
    return table


def crc32c(data: bytes) -> int:
    """Compute CRC-32C (Castagnoli) checksum of *data*.

    Returns a ``u32`` as a Python ``int``.
    """
    global _TABLE
    if _TABLE is None:
        _TABLE = _make_table()
    crc = 0xFFFFFFFF
    for byte in data:
        crc = _TABLE[(crc ^ byte) & 0xFF] ^ (crc >> 8)
    return crc ^ 0xFFFFFFFF
