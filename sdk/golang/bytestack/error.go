package bytestack

import "fmt"

// ErrorKind classifies Bytestack SDK errors.
type ErrorKind string

const (
	KindStackClosed      ErrorKind = "stack-closed"
	KindStackFull        ErrorKind = "stack-full"
	KindController       ErrorKind = "controller"
	KindSerialize        ErrorKind = "serialize"
	KindInvalidStack     ErrorKind = "invalid-stack"
	KindStackIncomplete  ErrorKind = "stack-incomplete"
	KindMagicMismatch    ErrorKind = "magic-mismatch"
	KindChecksumMismatch ErrorKind = "checksum-mismatch"
	KindInternal         ErrorKind = "internal"
	KindIO               ErrorKind = "io"
)

// Error is a structured SDK error.
type Error struct {
	Kind    ErrorKind
	Message string
	Err     error // wrapped error, if any
}

func (e *Error) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: %s: %v", e.Kind, e.Message, e.Err)
	}
	return fmt.Sprintf("%s: %s", e.Kind, e.Message)
}

func (e *Error) Unwrap() error { return e.Err }

// Sentinel errors.
var (
	ErrStackClosed = &Error{Kind: KindStackClosed, Message: "stack is closed, no further writes allowed"}
)

// StackFullError is returned when the data file would exceed the size limit.
type StackFullError struct {
	Current int
	MaxSize int
}

func (e *StackFullError) Error() string {
	return fmt.Sprintf("stack full: %d bytes >= %d byte limit", e.Current, e.MaxSize)
}

// IsStackFull reports whether err is a StackFullError.
func IsStackFull(err error) bool {
	_, ok := err.(*StackFullError)
	return ok
}

// ControllerError wraps controller gRPC errors.
type ControllerError struct {
	Err error
}

func (e *ControllerError) Error() string {
	return fmt.Sprintf("controller error: %v", e.Err)
}

func (e *ControllerError) Unwrap() error { return e.Err }

type MagicMismatchError struct {
	Expected uint64
	Got      uint64
	Context  string
}

func (e *MagicMismatchError) Error() string {
	return fmt.Sprintf("magic mismatch in %s: expected %d, got %d", e.Context, e.Expected, e.Got)
}

type ChecksumMismatchError struct {
	Expected uint32
	Actual   uint32
}

func (e *ChecksumMismatchError) Error() string {
	return fmt.Sprintf("checksum mismatch: expected %#x, got %#x", e.Expected, e.Actual)
}

type StackIncompleteError struct {
	Message string
}

func (e *StackIncompleteError) Error() string {
	if e.Message == "" {
		return "stack incomplete"
	}
	return "stack incomplete: " + e.Message
}

type InternalError struct {
	Message string
	Err     error
}

func (e *InternalError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("internal error: %s: %v", e.Message, e.Err)
	}
	return "internal error: " + e.Message
}

func (e *InternalError) Unwrap() error { return e.Err }
