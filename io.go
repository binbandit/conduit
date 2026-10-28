package conduit

import (
	"fmt"
	"io"
)

// LimitedReader is an io.Reader that enforces a maximum read limit.
// It never reads past 'limit' bytes. After that, Read returns an error until Reset.
//
// This is useful for preventing large malicious payloads from consuming too much memory.
type LimitedReader struct {
	r        io.Reader
	limit    int64
	consumed int64
}

// NewLimitedReader creates a new LimitedReader that wraps the provided io.Reader 'r'
// and enforces a maximum allowed read size of 'limit' bytes.
func NewLimitedReader(r io.Reader, limit int64) *LimitedReader {
	return &LimitedReader{
		r:     r,
		limit: limit,
	}
}

// Read reads into p, stopping at the limit. Past the limit it returns 0 bytes and an error.
// Never return data together with the error: the jsonv2-backed decoder ignores those errors.
func (l *LimitedReader) Read(p []byte) (n int, err error) {
	remaining := l.limit - l.consumed
	if remaining <= 0 {
		return 0, fmt.Errorf("message size exceeds limit of %d bytes", l.limit)
	}
	if int64(len(p)) > remaining {
		p = p[:remaining]
	}
	n, err = l.r.Read(p)
	l.consumed += int64(n)
	return n, err
}

// Reset restarts the limit. Call it before each message.
func (l *LimitedReader) Reset() {
	l.consumed = 0
}
