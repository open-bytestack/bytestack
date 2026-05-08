# Bytestack Storage Format v1.0 — RFC

> **Status:** Draft  
> **Last updated:** 2026-05-04  
> **Reference implementation:** [Rust SDK](https://github.com/dashjay/bytestack/tree/master/core/src)

---

## 1. Overview

Bytestack is a binary container format designed for bundling billions of small files into a small number of large blobs, inspired by Facebook's Haystack. A Bytestack **Stack** is the minimal logical unit — exactly three co-located files sharing the same `stack_id`.

| File | Suffix | Purpose |
|------|--------|---------|
| Data  | `.data` | Raw payload bytes, 4 KiB-aligned records |
| Index | `.idx`  | Fixed-size binary index for O(1) lookup |
| Meta  | `.meta` | Newline-delimited JSON metadata |

**Path convention** (all three files share the same prefix):

```
{prefix}0x{stack_id}.data
{prefix}0x{stack_id}.idx
{prefix}0x{stack_id}.meta
```

`stack_id` is formatted as a zero-padded 4-digit lowercase hex number (`0x{:04x}`).  
The prefix may be an S3 URI (`s3://bucket/key`) or a local filesystem path.

**Stack ID provenance.** A `stack_id` originates from one of two sources:

| Mode | File name `stack_id` | Header `stack_id` | When used |
|------|----------------------|-------------------|-----------|
| **Controller** | Allocated by Controller gRPC (`next_stack_id`) | Same as file name | Production / S3 |
| **Local** (offline) | Current Unix timestamp (seconds) | `u64::MAX` (`0xFFFF_FFFF_FFFF_FFFF`) | Local development before S3 upload |

Stacks written in **local mode** MUST be migrated — file renames and header patches — before they are uploaded to S3 (see §8.4).

---

## 2. Data File (`.data`)

### 2.1 Layout

```
Offset 0:  +-----------------------------------------------+
           | DataMagicHeader  (16 bytes, bincode)           |
           +-----------------------------------------------+
           | Zero padding     (4080 bytes)                  |
           +-----------------------------------------------+
           | DataRecord #1    (aligned to 4096)             |
           +-----------------------------------------------+
           | DataRecord #2    (aligned to 4096)             |
           +-----------------------------------------------+
           | ...                                            |
           | DataRecord #N    (aligned to 4096)             |
           +-----------------------------------------------+
```

### 2.2 DataMagicHeader (bytes 0–15)

Serialized with **bincode** (little-endian).

```
Offset  Type   Field              Value        Description
------  ------ ------------------ ------------ ----------------------------
 0      u64    data_magic_number  47494638     Magic number (file-type identifier)
 8      u64    stack_id           variable     Links to matching .idx / .meta
```

Total: **16 bytes**. The header is written at offset 0, then the file is padded with zeros to exactly **4096 bytes**.

**Validation:** Every SDK MUST read the first 8 bytes and reject the file if `data_magic_number != 47494638`.

### 2.3 DataRecord

Every DataRecord occupies exactly one 4096-byte aligned slot. The slot layout is:

```
+---------------------------------------+
| DataRecordHeader   (20 bytes)          |
+---------------------------------------+
| Payload            (size bytes)        |
+---------------------------------------+
| Zero padding       (variable)         |
+---------------------------------------+
```

**Alignment formula:**

```
slot_size    = 4096
header_bytes = 20
payload_size = len(data)

raw_size = header_bytes + payload_size
padding  = (slot_size - (raw_size % slot_size)) % slot_size
```

### 2.4 DataRecordHeader (20 bytes)

Serialized with **bincode** (little-endian).

```
Offset  Type   Field                     Value     Description
------  ------ ------------------------- --------- -------------------------
 0      u32    data_magic_record_start   257758    Start sentinel
 4      u32    cookie                    random    Obfuscation token (§6.1)
 8      u32    size                      variable  Payload byte length
12      u32    crc                       computed  CRC-32C of payload (§6.2)
16      u32    data_magic_record_end    857752    End sentinel
```

Total: **20 bytes**.

**Validation:**  
- Start sentinel `!= 257758` → discard (magic mismatch)
- End sentinel `!= 857752` → discard  
- CRC mismatch → data corruption (see §7)

---

## 3. Index File (`.idx`)

### 3.1 Layout

```
Offset 0:  +----------------------------------------------------+
           | IndexMagicHeader   (16 bytes, bincode)              |
           +----------------------------------------------------+
           | IndexRecord #1     (28 bytes, bincode)              |
           +----------------------------------------------------+
           | IndexRecord #2     (28 bytes)                       |
           +----------------------------------------------------+
           | ...                                                 |
           | IndexRecord #N     (28 bytes)                       |
           +----------------------------------------------------+
```

No padding between records. The index is append-only.

### 3.2 IndexMagicHeader (16 bytes)

```
Offset  Type   Field                Value      Description
------  ------ -------------------- ---------- ----------------------------
 0      u64    index_header_magic   5201314    Identifies this as an .idx file
 8      u64    stack_id             variable   Links to matching .data / .meta
```

### 3.3 IndexRecord (28 bytes)

```
Offset  Type   Field         Description
------  ------ ------------- -----------------------------------------
 0      u32    cookie        Same cookie as the corresponding DataRecordHeader
 4      u64    offset_data   Byte offset of the DataRecord in the .data file
12      u32    size_data     Payload byte length (same as DataRecordHeader.size)
16      u64    offset_meta   Byte offset of the JSON MetaRecord in the .meta file
24      u32    size_meta     Byte length of the JSON MetaRecord (incl. trailing \n)
```

**Total: 28 bytes.**

### 3.4 IndexID — The Global Key

The public identifier returned by `put()` is called an **IndexID**. It is composed of three parts:

```
{stack_id},{offset_data:x}{cookie:08x}
```

| Part       | Format           | Example     |
|------------|------------------|-------------|
| stack_id   | decimal u64      | `100`       |
| offset_data| hex u64 (no pad) | `420fe000`  |
| cookie     | hex u32 (8-char) | `d0b8efae`  |

Full example: `100,420fe000d0b8efae`

The IndexID allows a reader to locate data directly without scanning: parse `stack_id` to find the stack directory, then use `offset_data` to seek into the `.data` file, and verify the record with `cookie`.

---

## 4. Meta File (`.meta`)

### 4.1 Layout

```
+----------------------------------------------------+
| MetaMagicHeader  (JSON + \n)                       |
+----------------------------------------------------+
| MetaRecord #1    (JSON + \n)                       |
+----------------------------------------------------+
| MetaRecord #2    (JSON + \n)                       |
+----------------------------------------------------+
| ...                                                |
+----------------------------------------------------+
```

Newline (`0x0A`) is the record delimiter.

### 4.2 MetaMagicHeader

JSON object, one line:

```json
{"meta_magic_number":1314920,"stack_id":<u64>}
```

**Validation:** Parse the JSON, check `meta_magic_number == 1314920`.

### 4.3 MetaRecord

One JSON object per line:

```json
{"create_time":<u64>,"offset_data":<u64>,"size_data":<u32>,"cookie":<u32>,"filename":"<string>","extra":<bytes>}
```

| Field        | Type    | Content                              |
|--------------|---------|--------------------------------------|
| create_time  | u64     | Unix timestamp (seconds)             |
| offset_data  | u64     | Byte offset in .data file            |
| size_data    | u32     | Payload byte length                  |
| cookie       | u32     | Random obfuscation token             |
| filename     | String  | Original file name (UTF-8)           |
| extra        | [u8]    | Arbitrary application metadata (JSON, protobuf, etc.) |

---

## 5. Magic Numbers Reference

| Sentinel               | Value    | Location              |
|------------------------|----------|-----------------------|
| Data file magic        | 47494638 | .data offset 0        |
| Data record start      | 257758   | DataRecordHeader[0:4] |
| Data record end        | 857752   | DataRecordHeader[16:20] |
| Index file magic       | 5201314  | .idx offset 0         |
| Meta file magic        | 1314920  | .meta first JSON      |

---

## 6. Packing Constraints

### 6.1 Cookie Generation

Each call to `put()` MUST generate a random `u32` cookie. The cookie is stored in three places:
- `DataRecordHeader.cookie`
- `IndexRecord.cookie`
- `MetaRecord.cookie`

**Purpose:** Prevents offset guessing / collision attacks. An attacker who knows only the offset cannot reconstruct a valid IndexID without the cookie.

**Implementation:** Use a cryptographically secure PRNG (`rand::thread_rng()` in Rust, `crypto/rand` in Go, `secrets` in Python).

### 6.2 Checksum

Each data payload MUST be checksummed with **CRC-32C (Castagnoli)**, polynomial `0x1EDC6F41` (ISO 3309 / CRC-32/ISCSI).

- Computed over the raw payload **only** (not the header, not the padding).
- Stored in `DataRecordHeader.crc`.
- On read, the CRC MUST be recomputed and compared. A mismatch is treated as data corruption (§7).

### 6.3 Write Atomicity

1. The `.data` payload for a record MUST be fully written and flushed to the active storage backend **before** the corresponding IndexRecord is persisted to `.idx`.
2. The `.meta` file write order relative to `.idx` is not critical — both reference each other via offsets and cookies, so consistency is enforced by post-read validation.

### 6.4 Data File Size Limit

The default maximum capacity of a single `.data` file is **5 GiB** (5 × 1024³ bytes). When the next `put()` would exceed this threshold:

1. `close()` the current stack (flush all three files).
2. Allocate a new `stack_id`:
   - controller mode: request a new `stack_id` from the Controller
   - local mode: generate a fresh timestamp-based file `stack_id`
3. Open a new stack and continue writing.

This limit SHOULD be configurable at the SDK level.

---

## 7. Error Handling

All multi-language SDKs MUST implement these error-handling rules identically.

### 7.1 Magic Mismatch

| Scenario | Behavior |
|----------|----------|
| `.data` magic number != 47494638 | **Fatal error.** The file is not a valid Bytestack data file. SDK MUST NOT attempt to read any records from it. |
| `.idx` magic number != 5201314 | **Fatal error.** The index file is invalid. |
| `.meta` magic JSON != 1314920 | **Fatal error.** The metadata file is invalid. |
| DataRecordHeader start magic != 257758 | **Record-level error.** This record is corrupt or a seek misalignment occurred. SDK MUST skip to the next aligned boundary or abort iteration depending on the read mode. |
| DataRecordHeader end magic != 857752 | Same as above. |

**Recommended error type:** `BytestackError::MagicMismatch { expected: u64, got: u64, context: &str }`

### 7.2 CRC Corruption

If the recomputed CRC-32C does not match `DataRecordHeader.crc`:

- **During `put()`**: This SHOULD NOT happen (the CRC was just computed). Treat it as a software bug.
- **During read (bsserver or direct)**: Return a `BytestackError::ChecksumMismatch` containing the expected and actual checksums. The caller decides whether to retry from a replica or propagate the error.

### 7.3 File Not Found

If any one of the three stack files (`.data`, `.idx`, `.meta`) is missing, the entire stack is considered incomplete. Open/read operations MUST return a `BytestackError::StackIncomplete`.

### 7.4 Error Taxonomy

```
BytestackError
├── MagicMismatch    — Magic number validation failure
├── ChecksumMismatch — CRC-32C mismatch
├── StackIncomplete  — One or more stack files missing
├── IOError          — Underlying storage I/O failure
├── StackFull        — Data file exceeds size limit
└── Internal         — Unexpected internal condition
```

---

## 8. Interface Contract

Every language SDK MUST expose these three core functions.

### 8.1 `open_writer(path, controller?) -> Writer`

| Parameter   | Type            | Description                                      |
|-------------|-----------------|--------------------------------------------------|
| path        | `string`        | Storage URI (e.g. `s3://bucket/prefix/` or `/local/data/`) |
| controller  | `string?` (opt) | Controller gRPC address (e.g. `http://localhost:8080`) |

**Behavior:**

1. If `controller` is provided:
   - Connect to the Controller gRPC service.
   - Call `Controller.next_stack_id()` to obtain a fresh `stack_id`.
   - Use this `stack_id` for both file naming and binary headers.
2. If `controller` is `None` (local mode):
   - Use the current Unix timestamp (seconds since epoch) as the file-naming `stack_id`.
   - Write `u64::MAX` (`0xFFFF_FFFF_FFFF_FFFF`) into all binary headers as the `stack_id` field, marking the data as *generated locally*.
3. Create empty `.data`, `.idx`, `.meta` files with their magic headers.

### 8.2 `put(data, filename, extra_meta) -> index_id`

| Parameter  | Type            | Description                        |
|------------|-----------------|------------------------------------|
| data       | `[]byte`        | Raw payload bytes                  |
| filename   | `string`        | Original file name                 |
| extra_meta | `[]byte` (opt)  | Arbitrary metadata blob, may be empty |

**Returns:**  
`index_id` — a globally unique string in the format `{stack_id},{hex_offset}{hex_cookie}` (see §3.4).

**Lifecycle:**
1. Generate random `cookie`.
2. Compute CRC-32C of `data`.
3. Serialize and write `DataRecordHeader` + `data` + padding → `.data`
4. Flush `.data` to the active backend.
5. Serialize and write `MetaRecord` (JSON + `\n`) → `.meta`
6. Serialize and write `IndexRecord` → `.idx`
7. Return `index_id`.

### 8.3 `close()`

1. Flush all buffered data to the storage backend.
2. Close all three file handles.
3. If the stack contains no records (only headers), the files MAY be deleted to avoid empty stacks.

### 8.4 Local Mode & Migration

When writing without a Controller, the stack is in **local mode**:

| Artifact | Detail |
|----------|--------|
| File names | `0x{unix_timestamp}.{suffix}` |
| Header `stack_id` | `u64::MAX` (all three magic headers) |
| IndexID returned by `put()` | `{timestamp},{hex_offset}{hex_cookie}` |
| Mutability | Once `close()` is called, the writer MUST reject further writes (`StackClosed` error). |

**Migration for S3 upload.** Before a local-mode stack can be served from S3:

1. Connect to a Controller and call `next_stack_id` to obtain a real `stack_id`.
2. Rename the three files: `0x{timestamp}.{suffix}` → `0x{real_id}.{suffix}`.
3. Patch the `stack_id` field inside each binary header (`.data` offset 8–15, `.idx` offset 8–15, `.meta` first JSON object) from `u64::MAX` to the real `stack_id`.
4. Upload the renamed files to S3.

This migration step is **not** handled by the writer SDK; it is the caller's responsibility.

---

## 9. S3 Interaction Model

| Operation | SDK Role | Notes |
|-----------|----------|-------|
| Write (`.data`) | SDK writes sequentially via storage writer | Append semantics; SDK MUST flush before publishing `.idx` |
| Write (`.idx`)  | SDK appends IndexRecords | Small, fixed-size writes |
| Write (`.meta`) | SDK appends JSON lines | One line per record |
| Read (single)   | SDK can read for sampling/verification | For production, redirect to bsserver |
| Read (batch)    | **bsserver only** | SDK SHOULD NOT do batch reads from S3 directly |
| Range scan      | **bsserver only** | High latency / cost via S3 |

**Rule:** SDKs are responsible for writing and lightweight point-reads (e.g., integrity checks). All bulk or production read workloads MUST be routed through **bsserver**, which provides optimized gRPC fetch endpoints (`fetch_one`, `fetch_batch`, `range_from`).

For S3-compatible backends, an SDK MAY buffer record writes client-side until `close()` as long as object publication order preserves the atomicity rule: `.data` MUST become visible before `.idx`, and `.meta` MAY be published before or after `.idx`.

---

## 10. Binary Serialization Reference

| Context     | Format      | Endianness    |
|-------------|-------------|---------------|
| Magic headers | bincode   | Little-endian |
| DataRecordHeader | bincode | Little-endian |
| IndexRecord  | bincode     | Little-endian |
| MetaRecord   | JSON        | UTF-8         |
| CRC-32C      | u32 (raw)   | Little-endian |

### Equivalent in other languages:

- **Go:** `encoding/binary` with `binary.LittleEndian`
- **Python:** `struct.pack('<IqII...', ...)` or use `bincode` / manual packing
- **C/C++/Java:** Direct struct packing with little-endian byte order

---

## 11. Reference Constants

| Constant                        | Value                  | Description                         |
|---------------------------------|------------------------|-------------------------------------|
| `ALIGNMENT_SIZE`                | 4096                   | Record alignment boundary (bytes)   |
| `DATA_MAGIC`                    | 47494638               | .data file magic                    |
| `INDEX_MAGIC`                   | 5201314                | .idx file magic                     |
| `META_MAGIC`                    | 1314920                | .meta file magic                    |
| `RECORD_MAGIC_START`            | 257758                 | Data record start sentinel          |
| `RECORD_MAGIC_END`              | 857752                 | Data record end sentinel            |
| `DATA_HEADER_SIZE`              | 4096                   | .data file header (incl. padding)   |
| `DATA_RECORD_HEADER_SIZE`       | 20                     | Per-record header size              |
| `INDEX_HEADER_SIZE`             | 16                     | .idx file magic header              |
| `INDEX_RECORD_SIZE`             | 28                     | Per-index-record size               |
| `MAX_DATA_FILE_SIZE`            | 5 × 1024³ (5 GiB)     | Max .data file before rotation      |
| `CRC_POLYNOMIAL`                | `0x1EDC6F41`           | CRC-32C (Castagnoli)                |
| `LOCAL_STACK_ID`                | `u64::MAX`             | Sentinel in headers for local mode  |

---

## A. Appendix — ASCII Flow: Write Path

```
put(data, filename, extra_meta)
│
├─ 1. Generate random cookie (u32)
├─ 2. Compute CRC-32C of data
├─ 3. Build DataRecord, write to .data
├─ 4. Flush .data to active backend
├─ 5. Build MetaRecord, write to .meta
├─ 6. Build IndexRecord, write to .idx
│
└─ return "{stack_id},{offset:x}{cookie:08x}"
```

**Flush order:** .data → (.meta optional) → .idx

---

## B. Appendix — ASCII Flow: Read Path (via bsserver)

```
client: fetch_one(index_id)
  │
  ├─ Parse stack_id → locate stack + bsserver
  ├─ Parse offset + cookie from index_id
  ├─ Seek .data → offset
  ├─ Read DataRecordHeader → validate magic + CRC
  ├─ Read payload
  │
  └─ return payload (or error)
```

---

## C. Appendix — SDK Checklist (for LLM-assisted code generation)

When generating a new SDK implementation, validate against this checklist:

- [ ] Three-file structure: `.data` / `.idx` / `.meta` with correct naming (`0x{stack_id:04x}.{suffix}`)
- [ ] Data header: 16-byte bincode magic + 4080-byte zero pad
- [ ] DataRecord: 20-byte header (start magic + cookie + size + CRC + end magic) + payload + zero pad to 4096
- [ ] Index header: 16-byte bincode magic
- [ ] IndexRecord: 28-byte fixed-size bincode struct (cookie + offset_data:u64 + size_data:u32 + offset_meta:u64 + size_meta:u32)
- [ ] Meta header: JSON magic + `\n`
- [ ] MetaRecord: JSON one-liner + `\n` (create_time + offset_data + size_data + cookie + filename + extra)
- [ ] CRC-32C (Castagnoli, `0x1EDC6F41`) computed on raw payload only
- [ ] Random u32 cookie per record
- [ ] IndexID format: `{stack_id},{offset_data:x}{cookie:08x}`
- [ ] Data file size limit (default 5 GiB, configurable)
- [ ] Atomically flush data file before index update
- [ ] Validate all five magic constants on read
- [ ] Bubble errors as the taxonomy in §7.4
- [ ] Local mode: timestamp-based file naming, `u64::MAX` in headers
- [ ] Controller mode: real `stack_id` from `next_stack_id` gRPC
- [ ] Writer enforces close-once — `put()` returns `StackClosed` after `close()`
