package ingestion

import (
	"errors"
	"fmt"
	"strings"
)

// ErrHandlerPanic is wrapped by every HandlerPanicError. A panic is a bug, not
// a transient fault, so it is classified as a permanent failure.
var ErrHandlerPanic = errors.New("ingestion: handler panic")

// ErrWorkerPanic marks a panic that escaped the per-record boundary and was
// caught by the worker loop's last line of defence. It is reported as fatal.
var ErrWorkerPanic = errors.New("ingestion: partition worker panic")

const maxPanicTextRunes = 500

// HandlerPanicError is a panic raised by a topic handler, converted into an
// error at the worker boundary so one bad record cannot take down the process.
type HandlerPanicError struct {
	Topic     string
	Partition int32
	Offset    int64
	Value     any
	Stack     []byte
}

func (e *HandlerPanicError) Error() string {
	return fmt.Sprintf("handler panic at %s/%d offset %d: %s",
		e.Topic, e.Partition, e.Offset, panicText(e.Value))
}

func (e *HandlerPanicError) Unwrap() error { return ErrHandlerPanic }

// panicText renders a panic value as bounded, valid UTF-8 so it can be logged
// and stored in a TEXT column without ever failing the write.
func panicText(v any) string {
	s := strings.ToValidUTF8(fmt.Sprint(v), "\uFFFD")
	s = strings.ReplaceAll(s, "\x00", "")
	if r := []rune(s); len(r) > maxPanicTextRunes {
		s = string(r[:maxPanicTextRunes]) + "…"
	}
	return s
}
