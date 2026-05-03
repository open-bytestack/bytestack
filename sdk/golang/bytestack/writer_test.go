package bytestack

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func tmpDir(t *testing.T) string {
	t.Helper()
	d, err := os.MkdirTemp("", "bst_sdk_test_*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(d) })
	return d
}

func TestLocalModeHeadersUseU64Max(t *testing.T) {
	dir := tmpDir(t)
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	sid := w.StackID()
	if sid < 1_700_000_000 {
		t.Fatalf("expected a Unix timestamp, got %d", sid)
	}
	if w.HeaderStackID() != math.MaxUint64 {
		t.Fatalf("header stack_id should be u64::MAX, got %#x", w.HeaderStackID())
	}

	if _, err := w.Put([]byte("hello"), "t.txt", nil); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Files should be named with the timestamp, not u64::MAX.
	dataFile := filepath.Join(dir, fmt.Sprintf("0x%04x.data", sid))
	if _, err := os.Stat(dataFile); os.IsNotExist(err) {
		t.Fatal("data file should use timestamp-based name")
	}
	maxFile := filepath.Join(dir, "0xffffffffffffffff.data")
	if _, err := os.Stat(maxFile); !os.IsNotExist(err) {
		t.Fatal("data file must NOT be named with u64::MAX")
	}

	// Verify the binary header actually contains u64::MAX.
	raw, err := os.ReadFile(dataFile)
	if err != nil {
		t.Fatal(err)
	}
	stored := binary.LittleEndian.Uint64(raw[8:16])
	if stored != math.MaxUint64 {
		t.Fatalf("expected u64::MAX in header, got %#x", stored)
	}
}

func TestPutAndCloseLocal(t *testing.T) {
	dir := tmpDir(t)
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	sid := w.StackID()

	id, err := w.Put([]byte("hello world"), "test.txt", nil)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(id, fmt.Sprintf("%d,", sid)) {
		t.Fatalf("index_id %q should start with %d,", id, sid)
	}

	id2, err := w.Put([]byte("foobar"), "bar.txt", []byte("{}"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(id2, fmt.Sprintf("%d,", sid)) {
		t.Fatalf("index_id %q should start with %d,", id2, sid)
	}
	if id == id2 {
		t.Fatal("two puts should return different index_ids")
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	for _, suffix := range []string{"data", "idx", "meta"} {
		p := filepath.Join(dir, fmt.Sprintf("0x%04x.%s", sid, suffix))
		if _, err := os.Stat(p); os.IsNotExist(err) {
			t.Fatalf("missing stack file: %s", p)
		}
	}

	// put after close must fail.
	_, err = w.Put([]byte("nope"), "late.txt", nil)
	if err != ErrStackClosed {
		t.Fatalf("expected ErrStackClosed, got %v", err)
	}
}

func TestSizeLimitEnforced(t *testing.T) {
	dir := tmpDir(t)
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	w.maxDataBytes = 100

	if _, err := w.Put(make([]byte, 90), "large.txt", nil); err != nil {
		t.Fatal(err)
	}
	_, err = w.Put(make([]byte, 20), "overflow.txt", nil)
	if !IsStackFull(err) {
		t.Fatalf("expected StackFullError, got %v", err)
	}
	w.Close()
}

func TestControllerConnectFailure(t *testing.T) {
	dir := tmpDir(t)
	_, err := Open(dir, "http://127.0.0.1:1")
	if err == nil {
		t.Fatal("expected error connecting to non-existent controller")
	}
}

func TestBinaryFormat(t *testing.T) {
	dir := tmpDir(t)
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	sid := w.StackID()

	// Write 3 records of varying sizes.
	w.Put([]byte("hello world"), "f1.txt", nil)
	w.Put([]byte("this is a larger payload for testing"), "f2.txt", nil)
	w.Put([]byte("tiny"), "f3.txt", nil)
	w.Close()

	dataPath := filepath.Join(dir, fmt.Sprintf("0x%04x.data", sid))
	idxPath := filepath.Join(dir, fmt.Sprintf("0x%04x.idx", sid))
	metaPath := filepath.Join(dir, fmt.Sprintf("0x%04x.meta", sid))

	// --- Data file ---
	dataBytes, _ := os.ReadFile(dataPath)
	// Header: 16-byte magic + 4080 zero padding = 4096
	if len(dataBytes) < DataHeaderSize {
		t.Fatalf("data file too small: %d", len(dataBytes))
	}
	// Each record should be 4096 bytes aligned
	expectedSize := DataHeaderSize + 3*Alignment
	if len(dataBytes) != expectedSize {
		t.Fatalf("data file size: got %d, want %d", len(dataBytes), expectedSize)
	}

	// Verify record headers.
	for i := 0; i < 3; i++ {
		off := DataHeaderSize + i*Alignment
		magicStart := binary.LittleEndian.Uint32(dataBytes[off:])
		magicEnd := binary.LittleEndian.Uint32(dataBytes[off+16:])
		if magicStart != RecordMagicStart {
			t.Fatalf("record %d: bad start magic %d", i, magicStart)
		}
		if magicEnd != RecordMagicEnd {
			t.Fatalf("record %d: bad end magic %d", i, magicEnd)
		}
	}

	// --- Index file ---
	idxBytes, _ := os.ReadFile(idxPath)
	// Magic header (16) + 3 records * 28.
	expectedIdxSize := 16 + 3*28
	if len(idxBytes) != expectedIdxSize {
		t.Fatalf("idx file size: got %d, want %d", len(idxBytes), expectedIdxSize)
	}
	idxMagic := binary.LittleEndian.Uint64(idxBytes[0:8])
	if idxMagic != IndexMagic {
		t.Fatalf("idx magic: got %d, want %d", idxMagic, IndexMagic)
	}
	idxStackID := binary.LittleEndian.Uint64(idxBytes[8:16])
	if idxStackID != LocalStackID {
		t.Fatalf("idx stack_id: got %#x, want %#x", idxStackID, LocalStackID)
	}

	// --- Meta file ---
	metaBytes, _ := os.ReadFile(metaPath)
	lines := strings.Split(strings.TrimSpace(string(metaBytes)), "\n")
	if len(lines) != 4 { // header + 3 records
		t.Fatalf("meta lines: got %d, want 4", len(lines))
	}
	var mh metaMagicHeader
	if err := json.Unmarshal([]byte(lines[0]), &mh); err != nil {
		t.Fatal(err)
	}
	if mh.MetaMagicNumber != MetaMagic {
		t.Fatalf("meta magic: got %d, want %d", mh.MetaMagicNumber, MetaMagic)
	}
	if mh.StackID != LocalStackID {
		t.Fatalf("meta stack_id: got %#x, want %#x", mh.StackID, LocalStackID)
	}
}

func TestMultipleCloseIsSafe(t *testing.T) {
	dir := tmpDir(t)
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal("second close should be a no-op")
	}
}
