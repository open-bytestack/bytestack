package bytestack

import "fmt"

// ErrorKind classifies Bytestack SDK errors.
type ErrorKind string

const (
	KindStackClosed  ErrorKind = "stack-closed"
	KindStackFull    ErrorKind = "stack-full"
	KindController   ErrorKind = "controller"
	KindSerialize    ErrorKind = "serialize"
	KindInvalidStack ErrorKind = "invalid-stack"
	KindIO           ErrorKind = "io"
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
