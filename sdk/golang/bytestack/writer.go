// Package bytestack implements a directory writer for the Bytestack
// storage format, supporting local filesystem and S3 backends.
//
// # Example — local mode
//
//	package main
//
//	import (
//	    "log"
//	    "github.com/open-bytestack/bytestack/sdk/golang/bytestack"
//	)
//
//	func main() {
//	    w, err := bytestack.Open("/tmp/mystack")
//	    if err != nil { log.Fatal(err) }
//
//	    id, err := w.Put([]byte("hello world"), "greeting.txt", nil)
//	    if err != nil { log.Fatal(err) }
//	    log.Println("index_id:", id)
//
//	    if err := w.Close(); err != nil { log.Fatal(err) }
//	}
//
// # Example — controller mode
//
//	w, err := bytestack.Open("/tmp/mystack", "http://localhost:8080")
//
// # Example — S3 mode
//
//	w, err := bytestack.Open("s3://my-bucket/my-prefix")
//
// On-disk format (.data / .idx / .meta, binary layout, CRC-32C, alignment)
// is identical to the Rust reference implementation.
package bytestack

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"

	controllerpb "github.com/open-bytestack/bytestack/proto/src/controller"
)

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const (
	Alignment                  = 4096
	DataMagic                  = 47494638
	IndexMagic                 = 5201314
	MetaMagic                  = 1314920
	RecordMagicStart           = 257758
	RecordMagicEnd             = 857752
	DataHeaderSize             = 4096
	DataRecordHeaderLen        = 20
	IndexRecordLen             = 28
	LocalStackID        uint64 = 0xFFFF_FFFF_FFFF_FFFF
	DefaultMaxDataBytes        = 5 * 1024 * 1024 * 1024 // 5 GiB
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// ---------------------------------------------------------------------------
// JSON helpers
// ---------------------------------------------------------------------------

// extraBytes serialises as a JSON integer array (matching Rust serde Vec<u8>).
type extraBytes []byte

func (e extraBytes) MarshalJSON() ([]byte, error) {
	if len(e) == 0 {
		return []byte("[]"), nil
	}
	var buf bytes.Buffer
	buf.WriteByte('[')
	for i, b := range e {
		if i > 0 {
			buf.WriteByte(',')
		}
		fmt.Fprintf(&buf, "%d", b)
	}
	buf.WriteByte(']')
	return buf.Bytes(), nil
}

type metaMagicHeader struct {
	MetaMagicNumber uint64 `json:"meta_magic_number"`
	StackID         uint64 `json:"stack_id"`
}

type metaRecord struct {
	CreateTime uint64     `json:"create_time"`
	OffsetData uint64     `json:"offset_data"`
	SizeData   uint32     `json:"size_data"`
	Cookie     uint32     `json:"cookie"`
	Filename   string     `json:"filename"`
	Extra      extraBytes `json:"extra"`
}

// ---------------------------------------------------------------------------
// stackFileWriter — abstracts a single writable file in a stack
// ---------------------------------------------------------------------------

// stackFileWriter is the interface for writing to one of the three stack
// files (data, index, meta). Each implementation handles the underlying
// storage backend.
type stackFileWriter interface {
	io.Writer
	io.Closer
}

type stackFileSyncer interface {
	Sync() error
}

// stackWriterFactory creates stackFileWriter instances for a given
// filename suffix (e.g. "0x{id}.data").
type stackWriterFactory interface {
	CreateWriter(filename string) (stackFileWriter, error)
}

// ---------------------------------------------------------------------------
// Writer
// ---------------------------------------------------------------------------

// Writer manages one bytestack, storing records across three files.
//
// Lifecycle:
//
//	w, _ := bytestack.Open("/tmp/mystack")
//	id, _ := w.Put([]byte("hello"), "greeting.txt", nil)
//	w.Close()
//	// w.Put(...) returns ErrStackClosed.
type Writer struct {
	location       string // original location string
	factory        stackWriterFactory
	controllerAddr string
	fileStackID    uint64
	headerStackID  uint64
	dataFile       stackFileWriter
	idxFile        stackFileWriter
	metaFile       stackFileWriter
	dataOffset     uint64
	metaOffset     uint64
	totalRawBytes  int
	maxDataBytes   int
	closed         bool
}

// LocalWriter is an alias for Writer for backward compatibility.
type LocalWriter = Writer

// Open creates or opens a stack at the given location.
//
// Location prefixes:
//   - "/absolute/path"  → local filesystem
//   - "file:///path"     → local filesystem
//   - "./relative/path"  → local filesystem
//   - "s3://bucket/prefix" → S3 (requires AWS credentials in the environment
//     or ~/.aws/config; configure region via AWS_REGION, endpoint via
//     BYTESTACK_S3_ENDPOINT for MinIO-compatible stores)
//
// Optional controllerAddr enables controller-gRPC mode for stack-id
// assignment.
func Open(location string, controllerAddr ...string) (*Writer, error) {
	scheme, path, err := parseLocation(location)
	if err != nil {
		return nil, err
	}

	addr := ""
	if len(controllerAddr) > 0 {
		addr = controllerAddr[0]
	}

	var factory stackWriterFactory
	switch scheme {
	case "file":
		if err := os.MkdirAll(path, 0755); err != nil {
			return nil, fmt.Errorf("mkdir %s: %w", path, err)
		}
		factory = &localWriterFactory{dir: path}
	case "s3":
		if addr == "" {
			return nil, fmt.Errorf("s3 backend requires a controller address for stack-id assignment; s3:// stacks cannot use temporary local IDs")
		}
		bucket, prefix := parseS3Location(path)
		factory, err = newS3WriterFactory(bucket, prefix)
		if err != nil {
			return nil, fmt.Errorf("s3: %w", err)
		}
	default:
		return nil, fmt.Errorf("unsupported location scheme %q in %q (use /path, file://path, or s3://bucket/prefix)", scheme, location)
	}

	return newWriter(location, factory, addr)
}

func OpenWriter(location string, controllerAddr ...string) (*Writer, error) {
	return Open(location, controllerAddr...)
}

// parseLocation extracts scheme and path from a location string.
func parseLocation(location string) (scheme, path string, err error) {
	if strings.HasPrefix(location, "file://") {
		return "file", location[7:], nil
	}
	if strings.HasPrefix(location, "s3://") {
		return "s3", location[5:], nil
	}
	// Absolute path or relative path — treat as local.
	if !strings.Contains(location, "://") {
		return "file", location, nil
	}
	return "", "", fmt.Errorf("cannot parse location: %s", location)
}

// parseS3Location splits "bucket/prefix" into bucket and prefix.
func parseS3Location(s3path string) (bucket, prefix string) {
	parts := strings.SplitN(s3path, "/", 2)
	bucket = parts[0]
	if len(parts) == 2 {
		prefix = parts[1]
	}
	return
}

// newWriter creates a Writer using the given factory and optional
// controller address.
func newWriter(location string, factory stackWriterFactory, controllerAddr string) (*Writer, error) {
	w := &Writer{
		location:       location,
		factory:        factory,
		controllerAddr: controllerAddr,
		maxDataBytes:   DefaultMaxDataBytes,
	}
	return w, w.openStack()
}

func (w *Writer) resolveStackIDs() (uint64, uint64, error) {
	if w.controllerAddr != "" {
		sid, err := nextStackID(w.controllerAddr)
		if err != nil {
			return 0, 0, err
		}
		return sid, sid, nil
	}

	sid := uint64(time.Now().Unix())
	if localFactory, ok := w.factory.(*localWriterFactory); ok {
		for {
			path := filepath.Join(localFactory.dir, fmt.Sprintf("0x%04x.data", sid))
			if _, err := os.Stat(path); os.IsNotExist(err) {
				break
			}
			sid++
		}
	}
	return sid, LocalStackID, nil
}

func (w *Writer) openStack() error {
	fileStackID, headerStackID, err := w.resolveStackIDs()
	if err != nil {
		return err
	}

	dataFile, err := w.factory.CreateWriter(fmt.Sprintf("0x%04x.data", fileStackID))
	if err != nil {
		return fmt.Errorf("create data: %w", err)
	}
	idxFile, err := w.factory.CreateWriter(fmt.Sprintf("0x%04x.idx", fileStackID))
	if err != nil {
		dataFile.Close()
		return fmt.Errorf("create idx: %w", err)
	}
	metaFile, err := w.factory.CreateWriter(fmt.Sprintf("0x%04x.meta", fileStackID))
	if err != nil {
		idxFile.Close()
		dataFile.Close()
		return fmt.Errorf("create meta: %w", err)
	}

	w.fileStackID = fileStackID
	w.headerStackID = headerStackID
	w.dataFile = dataFile
	w.idxFile = idxFile
	w.metaFile = metaFile
	w.dataOffset = DataHeaderSize
	w.totalRawBytes = 0

	var dh [16]byte
	binary.LittleEndian.PutUint64(dh[0:8], DataMagic)
	binary.LittleEndian.PutUint64(dh[8:16], w.headerStackID)
	if _, err := w.dataFile.Write(dh[:]); err != nil {
		return err
	}
	zp := make([]byte, DataHeaderSize-16)
	if _, err := w.dataFile.Write(zp); err != nil {
		return err
	}

	var ih [16]byte
	binary.LittleEndian.PutUint64(ih[0:8], IndexMagic)
	binary.LittleEndian.PutUint64(ih[8:16], w.headerStackID)
	if _, err := w.idxFile.Write(ih[:]); err != nil {
		return err
	}

	mh, _ := json.Marshal(metaMagicHeader{
		MetaMagicNumber: MetaMagic,
		StackID:         w.headerStackID,
	})
	mhLine := append(mh, '\n')
	if _, err := w.metaFile.Write(mhLine); err != nil {
		return err
	}
	w.metaOffset = uint64(len(mhLine))
	return nil
}

func syncWriter(w stackFileWriter) error {
	if syncer, ok := w.(stackFileSyncer); ok {
		return syncer.Sync()
	}
	return nil
}

func (w *Writer) rotate() error {
	if err := w.closeCurrentStack(); err != nil {
		return err
	}
	return w.openStack()
}

func (w *Writer) closeCurrentStack() error {
	for _, f := range []stackFileWriter{w.dataFile, w.metaFile, w.idxFile} {
		if f == nil {
			continue
		}
		if err := f.Close(); err != nil {
			return fmt.Errorf("close: %w", err)
		}
	}
	w.dataFile = nil
	w.metaFile = nil
	w.idxFile = nil
	return nil
}

// StackID returns the stack identifier used for file naming
// (0x{stack_id}.{suffix}). In local mode this is a Unix timestamp;
// in controller mode it is the real stack_id.
func (w *Writer) StackID() uint64 { return w.fileStackID }

// HeaderStackID returns the stack identifier written into binary headers.
// In local mode this is u64::MAX; in controller mode it equals StackID().
func (w *Writer) HeaderStackID() uint64 { return w.headerStackID }

// Location returns the location string passed to Open.
func (w *Writer) Location() string { return w.location }

// MaxDataBytes returns the maximum raw payload bytes allowed before rotation.
func (w *Writer) MaxDataBytes() int { return w.maxDataBytes }

// TotalRawBytes returns the raw payload bytes written so far.
func (w *Writer) TotalRawBytes() int { return w.totalRawBytes }

// Put writes one record into the stack and returns an index_id.
//
// extraMeta is an opaque byte blob stored in the meta record
// (e.g. a JSON or protobuf encoding). Pass nil when no metadata is needed.
func (w *Writer) Put(data []byte, filename string, extraMeta []byte) (string, error) {
	if w.closed {
		return "", ErrStackClosed
	}

	// --- Size guard --------------------------------------------------------
	if len(data) > w.maxDataBytes {
		return "", &StackFullError{Current: len(data), MaxSize: w.maxDataBytes}
	}

	newTotal := w.totalRawBytes + len(data)
	if newTotal > w.maxDataBytes {
		if err := w.rotate(); err != nil {
			return "", err
		}
	}

	// --- Build records -----------------------------------------------------

	// Cookie — cryptographically random u32.
	var cookie uint32
	if err := binary.Read(rand.Reader, binary.LittleEndian, &cookie); err != nil {
		return "", fmt.Errorf("rand cookie: %w", err)
	}

	// CRC-32C (Castagnoli) of raw payload.
	crc := crc32.Checksum(data, castagnoli)

	dataOff := w.dataOffset
	metaOff := w.metaOffset
	now := uint64(time.Now().Unix())

	// MetaRecord (JSON + newline).
	mr := metaRecord{
		CreateTime: now,
		OffsetData: dataOff,
		SizeData:   uint32(len(data)),
		Cookie:     cookie,
		Filename:   filename,
		Extra:      extraBytes(extraMeta),
	}
	mrJSON, err := json.Marshal(mr)
	if err != nil {
		return "", &Error{Kind: KindSerialize, Message: "meta record", Err: err}
	}
	mrLine := append(mrJSON, '\n')
	mrLineLen := len(mrLine)

	// IndexRecord (28 bytes, little-endian binary — matches Rust bincode).
	ir := make([]byte, IndexRecordLen)
	binary.LittleEndian.PutUint32(ir[0:4], cookie)
	binary.LittleEndian.PutUint64(ir[4:12], dataOff)
	binary.LittleEndian.PutUint32(ir[12:16], uint32(len(data)))
	binary.LittleEndian.PutUint64(ir[16:24], metaOff)
	binary.LittleEndian.PutUint32(ir[24:28], uint32(mrLineLen))

	// Pre-compute index_id.
	indexID := fmt.Sprintf("%d,%x%08x", w.fileStackID, dataOff, cookie)

	// DataRecord header (20 bytes).
	drHdr := make([]byte, DataRecordHeaderLen)
	binary.LittleEndian.PutUint32(drHdr[0:4], RecordMagicStart)
	binary.LittleEndian.PutUint32(drHdr[4:8], cookie)
	binary.LittleEndian.PutUint32(drHdr[8:12], uint32(len(data)))
	binary.LittleEndian.PutUint32(drHdr[12:16], crc)
	binary.LittleEndian.PutUint32(drHdr[16:20], RecordMagicEnd)

	// Padding to 4K alignment.
	rawSz := DataRecordHeaderLen + len(data)
	padSz := (Alignment - (rawSz % Alignment)) % Alignment

	// --- Write order: data -> meta -> idx ----------------------------------
	if _, err := w.dataFile.Write(drHdr); err != nil {
		return "", fmt.Errorf("write data hdr: %w", err)
	}
	if _, err := w.dataFile.Write(data); err != nil {
		return "", fmt.Errorf("write data: %w", err)
	}
	if padSz > 0 {
		if _, err := w.dataFile.Write(make([]byte, padSz)); err != nil {
			return "", fmt.Errorf("write padding: %w", err)
		}
	}
	if err := syncWriter(w.dataFile); err != nil {
		return "", fmt.Errorf("sync data: %w", err)
	}
	w.dataOffset += uint64(rawSz + padSz)

	if _, err := w.metaFile.Write(mrLine); err != nil {
		return "", fmt.Errorf("write meta: %w", err)
	}
	if err := syncWriter(w.metaFile); err != nil {
		return "", fmt.Errorf("sync meta: %w", err)
	}
	w.metaOffset += uint64(mrLineLen)

	if _, err := w.idxFile.Write(ir); err != nil {
		return "", fmt.Errorf("write idx: %w", err)
	}
	if err := syncWriter(w.idxFile); err != nil {
		return "", fmt.Errorf("sync idx: %w", err)
	}

	w.totalRawBytes += len(data)
	return indexID, nil
}

// Close flushes and closes the three stack files.
//
// After this call any further Put() returns ErrStackClosed.
// Calling Close() multiple times is safe (subsequent calls are no-ops).
func (w *Writer) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	return w.closeCurrentStack()
}

// ---------------------------------------------------------------------------
// local writer factory
// ---------------------------------------------------------------------------

type localWriterFactory struct {
	dir string
}

func (f *localWriterFactory) CreateWriter(filename string) (stackFileWriter, error) {
	path := filepath.Join(f.dir, filename)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	return &localFileWriter{file: file}, nil
}

type localFileWriter struct {
	file *os.File
}

func (w *localFileWriter) Write(p []byte) (int, error) {
	return w.file.Write(p)
}

func (w *localFileWriter) Sync() error {
	return w.file.Sync()
}

func (w *localFileWriter) Close() error {
	return w.file.Close()
}

// ---------------------------------------------------------------------------
// Controller gRPC helper
// ---------------------------------------------------------------------------

// nextStackID obtains a fresh stack_id from the Controller gRPC service.
func nextStackID(addr string) (uint64, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return 0, &ControllerError{Err: fmt.Errorf("dial %s: %w", addr, err)}
	}
	defer conn.Close()

	client := controllerpb.NewControllerClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := client.NextStackID(ctx, &emptypb.Empty{})
	if err != nil {
		return 0, &ControllerError{Err: fmt.Errorf("NextStackID: %w", err)}
	}
	return resp.GetStackId(), nil
}

// ---------------------------------------------------------------------------
// io helpers (verification — ensure the type implements io.Closer)
// ---------------------------------------------------------------------------

var _ io.Closer = (*Writer)(nil)
