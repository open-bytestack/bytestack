// Package bytestack implements a local directory writer for the Bytestack
// storage format.
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
	Alignment           = 4096
	DataMagic           = 47494638
	IndexMagic          = 5201314
	MetaMagic           = 1314920
	RecordMagicStart    = 257758
	RecordMagicEnd      = 857752
	DataHeaderSize      = 4096
	DataRecordHeaderLen = 20
	IndexRecordLen      = 28
	LocalStackID uint64 = 0xFFFF_FFFF_FFFF_FFFF
	DefaultMaxDataBytes = 5 * 1024 * 1024 * 1024 // 5 GiB
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
// LocalWriter
// ---------------------------------------------------------------------------

// LocalWriter manages one bytestack on the local filesystem.
//
// Lifecycle:
//
//	w, _ := bytestack.Open("/tmp/mystack")
//	id, _ := w.Put([]byte("hello"), "greeting.txt", nil)
//	w.Close()
//	// w.Put(...) returns ErrStackClosed.
type LocalWriter struct {
	dir           string
	fileStackID   uint64
	headerStackID uint64
	dataFile      *os.File
	idxFile       *os.File
	metaFile      *os.File
	dataOffset    uint64
	metaOffset    uint64
	totalRawBytes int
	maxDataBytes  int
	closed        bool
}

// Open creates or opens a stack inside dir.
//
// controllerAddr is optional. When provided (e.g. "http://localhost:8080"),
// the stack_id is obtained from the Controller gRPC service. In local mode
// (no controller) the file naming uses the current Unix timestamp and binary
// headers carry u64::MAX.
func Open(dir string, controllerAddr ...string) (*LocalWriter, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", dir, err)
	}

	w := &LocalWriter{
		dir:          dir,
		maxDataBytes: DefaultMaxDataBytes,
	}

	// Resolve stack ID -------------------------------------------------------
	if len(controllerAddr) > 0 && controllerAddr[0] != "" {
		sid, err := nextStackID(controllerAddr[0])
		if err != nil {
			return nil, err
		}
		w.fileStackID = sid
		w.headerStackID = sid
	} else {
		w.fileStackID = uint64(time.Now().Unix())
		w.headerStackID = LocalStackID
	}

	// Create files ----------------------------------------------------------
	dataPath := filepath.Join(dir, fmt.Sprintf("0x%04x.data", w.fileStackID))
	idxPath := filepath.Join(dir, fmt.Sprintf("0x%04x.idx", w.fileStackID))
	metaPath := filepath.Join(dir, fmt.Sprintf("0x%04x.meta", w.fileStackID))

	var err error
	w.dataFile, err = os.Create(dataPath)
	if err != nil {
		return nil, fmt.Errorf("create data: %w", err)
	}
	w.idxFile, err = os.Create(idxPath)
	if err != nil {
		return nil, fmt.Errorf("create idx: %w", err)
	}
	w.metaFile, err = os.Create(metaPath)
	if err != nil {
		return nil, fmt.Errorf("create meta: %w", err)
	}

	// --- Write magic headers -----------------------------------------------

	// Data file: 16-byte magic header + 4080 zero padding.
	var dh [16]byte
	binary.LittleEndian.PutUint64(dh[0:8], DataMagic)
	binary.LittleEndian.PutUint64(dh[8:16], w.headerStackID)
	if _, err := w.dataFile.Write(dh[:]); err != nil {
		return nil, err
	}
	zp := make([]byte, DataHeaderSize-16)
	if _, err := w.dataFile.Write(zp); err != nil {
		return nil, err
	}

	// Index file: 16-byte magic header.
	var ih [16]byte
	binary.LittleEndian.PutUint64(ih[0:8], IndexMagic)
	binary.LittleEndian.PutUint64(ih[8:16], w.headerStackID)
	if _, err := w.idxFile.Write(ih[:]); err != nil {
		return nil, err
	}

	// Meta file: JSON magic header + newline.
	mh, _ := json.Marshal(metaMagicHeader{
		MetaMagicNumber: MetaMagic,
		StackID:         w.headerStackID,
	})
	mhLine := append(mh, '\n')
	if _, err := w.metaFile.Write(mhLine); err != nil {
		return nil, err
	}

	w.dataOffset = DataHeaderSize
	w.metaOffset = uint64(len(mhLine))

	return w, nil
}

// StackID returns the stack identifier used for file naming
// (0x{stack_id}.{suffix}). In local mode this is a Unix timestamp;
// in controller mode it is the real stack_id.
func (w *LocalWriter) StackID() uint64 { return w.fileStackID }

// HeaderStackID returns the stack identifier written into binary headers.
// In local mode this is u64::MAX; in controller mode it equals StackID().
func (w *LocalWriter) HeaderStackID() uint64 { return w.headerStackID }

// Dir returns the local directory path.
func (w *LocalWriter) Dir() string { return w.dir }

// MaxDataBytes returns the maximum raw payload bytes allowed before rotation.
func (w *LocalWriter) MaxDataBytes() int { return w.maxDataBytes }

// TotalRawBytes returns the raw payload bytes written so far.
func (w *LocalWriter) TotalRawBytes() int { return w.totalRawBytes }

// Put writes one record into the stack and returns an index_id.
//
// extraMeta is an opaque byte blob stored in the meta record
// (e.g. a JSON or protobuf encoding). Pass nil when no metadata is needed.
func (w *LocalWriter) Put(data []byte, filename string, extraMeta []byte) (string, error) {
	if w.closed {
		return "", ErrStackClosed
	}

	// --- Size guard --------------------------------------------------------
	newTotal := w.totalRawBytes + len(data)
	if newTotal > w.maxDataBytes {
		return "", &StackFullError{Current: newTotal, MaxSize: w.maxDataBytes}
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

	// --- Write order: index -> meta -> data --------------------------------
	if _, err := w.idxFile.Write(ir); err != nil {
		return "", fmt.Errorf("write idx: %w", err)
	}
	if _, err := w.metaFile.Write(mrLine); err != nil {
		return "", fmt.Errorf("write meta: %w", err)
	}
	w.metaOffset += uint64(mrLineLen)

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
	w.dataOffset += uint64(rawSz + padSz)

	w.totalRawBytes += len(data)
	return indexID, nil
}

// Close flushes and closes the three stack files.
//
// After this call any further Put() returns ErrStackClosed.
// Calling Close() multiple times is safe (subsequent calls are no-ops).
func (w *LocalWriter) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true

	for _, f := range []*os.File{w.dataFile, w.idxFile, w.metaFile} {
		if f == nil {
			continue
		}
		if err := f.Sync(); err != nil {
			return fmt.Errorf("sync: %w", err)
		}
		if err := f.Close(); err != nil {
			return fmt.Errorf("close: %w", err)
		}
	}
	return nil
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

var _ io.Closer = (*LocalWriter)(nil)
