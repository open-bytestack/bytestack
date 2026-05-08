package bytestack

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	e2e "github.com/open-bytestack/bytestack/sdk/golang/e2e"
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

// ---------------------------------------------------------------------------
// Path parsing
// ---------------------------------------------------------------------------

func TestParseLocation(t *testing.T) {
	tests := []struct {
		input      string
		wantScheme string
		wantPath   string
		wantErr    bool
	}{
		{"/tmp/mystack", "file", "/tmp/mystack", false},
		{"file:///tmp/mystack", "file", "/tmp/mystack", false},
		{"file://./relative", "file", "./relative", false},
		{"./relative/path", "file", "./relative/path", false},
		{"relative/path", "file", "relative/path", false},
		{"s3://my-bucket/prefix", "s3", "my-bucket/prefix", false},
		{"s3://my-bucket", "s3", "my-bucket", false},
	}
	for _, tt := range tests {
		scheme, path, err := parseLocation(tt.input)
		if tt.wantErr {
			if err == nil {
				t.Errorf("parseLocation(%q) expected error", tt.input)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseLocation(%q): %v", tt.input, err)
			continue
		}
		if scheme != tt.wantScheme {
			t.Errorf("parseLocation(%q) scheme = %q, want %q", tt.input, scheme, tt.wantScheme)
		}
		if path != tt.wantPath {
			t.Errorf("parseLocation(%q) path = %q, want %q", tt.input, path, tt.wantPath)
		}
	}
}

func TestParseS3Location(t *testing.T) {
	tests := []struct {
		input      string
		wantBucket string
		wantPrefix string
	}{
		{"my-bucket/prefix/sub", "my-bucket", "prefix/sub"},
		{"my-bucket", "my-bucket", ""},
		{"my-bucket/", "my-bucket", ""},
	}
	for _, tt := range tests {
		bucket, prefix := parseS3Location(tt.input)
		if bucket != tt.wantBucket {
			t.Errorf("parseS3Location(%q) bucket = %q, want %q", tt.input, bucket, tt.wantBucket)
		}
		if prefix != tt.wantPrefix {
			t.Errorf("parseS3Location(%q) prefix = %q, want %q", tt.input, prefix, tt.wantPrefix)
		}
	}
}

// ---------------------------------------------------------------------------
// Location prefixes
// ---------------------------------------------------------------------------

func TestOpenFilePrefix(t *testing.T) {
	dir := tmpDir(t)
	w, err := OpenWriter("file://" + dir)
	if err != nil {
		t.Fatal(err)
	}
	if w.Location() != "file://"+dir {
		t.Fatalf("Location() = %q, want %q", w.Location(), "file://"+dir)
	}
	id, err := w.Put([]byte("hello"), "f.txt", nil)
	if err != nil {
		t.Fatal(err)
	}
	if id == "" {
		t.Fatal("expected non-empty index_id")
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	// Verify file exists on disk.
	dataFile := filepath.Join(dir, fmt.Sprintf("0x%04x.data", w.StackID()))
	if _, err := os.Stat(dataFile); os.IsNotExist(err) {
		t.Fatal("data file should exist on disk")
	}
}

func TestOpenRelativePath(t *testing.T) {
	dir := tmpDir(t)
	// Change to dir and open with relative path.
	orig, _ := os.Getwd()
	os.Chdir(dir)
	defer os.Chdir(orig)

	w, err := Open("./mystack")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Put([]byte("data"), "r.txt", nil); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	// Verify files in ./mystack.
	if _, err := os.Stat("mystack"); os.IsNotExist(err) {
		t.Fatal("mystack directory should exist")
	}
}

func TestOpenInvalidScheme(t *testing.T) {
	_, err := Open("ftp://example.com/path")
	if err == nil {
		t.Fatal("expected error for ftp:// scheme")
	}
}

// ---------------------------------------------------------------------------
// Local mode (backward-compatible)
// ---------------------------------------------------------------------------

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
	firstStackID := w.StackID()

	if _, err := w.Put(make([]byte, 20), "overflow.txt", nil); err != nil {
		t.Fatalf("expected automatic rotation, got %v", err)
	}
	if w.StackID() == firstStackID {
		t.Fatalf("expected stack rotation, stack id stayed at %d", firstStackID)
	}

	_, err = w.Put(make([]byte, 200), "too-large.txt", nil)
	if !IsStackFull(err) {
		t.Fatalf("expected StackFullError for oversized record, got %v", err)
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

// ---------------------------------------------------------------------------
// LocalWriter alias backward compatibility
// ---------------------------------------------------------------------------

func TestLocalWriterAlias(t *testing.T) {
	dir := tmpDir(t)
	var w *LocalWriter // use the alias
	var err error
	w, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Put([]byte("data"), "f.txt", nil); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

// ---------------------------------------------------------------------------
// Location method
// ---------------------------------------------------------------------------

func TestLocationMethod(t *testing.T) {
	dir := tmpDir(t)
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if w.Location() != dir {
		t.Fatalf("Location() = %q, want %q", w.Location(), dir)
	}
	w.Close()
}

// ---------------------------------------------------------------------------
// S3 integration tests (using gofakes3 via e2e package)
// ---------------------------------------------------------------------------

func TestS3Writer(t *testing.T) {
	srv, err := e2e.Start(true)
	if err != nil {
		t.Fatalf("e2e.Start: %v", err)
	}
	defer srv.Close()

	endpoint := srv.S3Endpoint()
	controllerAddr := srv.ControllerAddr()

	// Restore env vars after test.
	prevEnv := map[string]string{
		"AWS_ACCESS_KEY_ID":     os.Getenv("AWS_ACCESS_KEY_ID"),
		"AWS_SECRET_ACCESS_KEY": os.Getenv("AWS_SECRET_ACCESS_KEY"),
		"AWS_REGION":            os.Getenv("AWS_REGION"),
		"BYTESTACK_S3_ENDPOINT": os.Getenv("BYTESTACK_S3_ENDPOINT"),
	}
	defer func() {
		for k, v := range prevEnv {
			os.Setenv(k, v)
		}
	}()
	os.Setenv("AWS_ACCESS_KEY_ID", "dummy-access-key")
	os.Setenv("AWS_SECRET_ACCESS_KEY", "dummy-secret-key")
	os.Setenv("AWS_REGION", "us-east-1")
	os.Setenv("BYTESTACK_S3_ENDPOINT", endpoint)

	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion("us-east-1"))
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	s3client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})

	bucket := "bst-test-bucket"
	if _, err := s3client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}

	// --- Writer test -----------------------------------------------------------

	w, err := Open("s3://"+bucket+"/stacks", controllerAddr)
	if err != nil {
		t.Fatalf("Open s3: %v", err)
	}

	id, err := w.Put([]byte("hello s3"), "greeting.txt", nil)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	t.Logf("index_id: %s", id)

	id2, err := w.Put([]byte("second record"), "second.txt", []byte(`{"k":"v"}`))
	if err != nil {
		t.Fatalf("Put 2: %v", err)
	}
	t.Logf("index_id 2: %s", id2)

	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// --- Verification ----------------------------------------------------------

	sid := w.StackID()
	if sid != 1 {
		t.Errorf("expected stack_id=1 from mock controller, got %d", sid)
	}

	for _, suffix := range []string{".data", ".idx", ".meta"} {
		key := fmt.Sprintf("stacks/0x%04x%s", sid, suffix)
		_, err := s3client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			t.Errorf("object s3://%s/%s not found: %v", bucket, key, err)
		}
	}

	// Put after close must fail.
	if _, err := w.Put([]byte("late"), "late.txt", nil); err != ErrStackClosed {
		t.Errorf("expected ErrStackClosed, got %v", err)
	}

	// Verify data and idx headers contain the real stack ID (not u64::MAX).
	for _, suffix := range []string{".data", ".idx"} {
		key := fmt.Sprintf("stacks/0x%04x%s", sid, suffix)
		result, err := s3client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			t.Errorf("get %s: %v", key, err)
			continue
		}
		var hdr [16]byte
		result.Body.Read(hdr[:])
		result.Body.Close()
		storedID := binary.LittleEndian.Uint64(hdr[8:16])
		if storedID != sid {
			t.Errorf("%s header stack_id = %#x, want %#x", suffix, storedID, sid)
		}
	}

	// Verify meta file header.
	metaKey := fmt.Sprintf("stacks/0x%04x.meta", sid)
	metaResult, err := s3client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(metaKey),
	})
	if err != nil {
		t.Fatalf("get meta: %v", err)
	}
	var mh metaMagicHeader
	if err := json.NewDecoder(metaResult.Body).Decode(&mh); err != nil {
		t.Fatalf("decode meta header: %v", err)
	}
	metaResult.Body.Close()
	if mh.StackID != sid {
		t.Errorf("meta header stack_id = %#x, want %#x", mh.StackID, sid)
	}
}

func TestS3WithoutControllerFails(t *testing.T) {
	_, err := Open("s3://my-bucket/my-prefix")
	if err == nil {
		t.Fatal("expected error for S3 without controller address")
	}
}
