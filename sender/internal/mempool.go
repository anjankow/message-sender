package internal

import (
	"bytes"
	"sync"
)

// MemPool is a pool of bytes.Buffer for reuse.
// Controlling its size is the responsibility of the caller.
type MemPool struct {
	pool *sync.Pool
}

func NewMemPool() MemPool {
	return MemPool{
		pool: &sync.Pool{
			New: func() any {
				return new(bytes.Buffer)
			},
		},
	}
}

func (p *MemPool) GetBuffer() *bytes.Buffer {
	buf := p.pool.Get().(*bytes.Buffer)
	return buf
}

// ReleaseBuffer cleans the buffer (keeping its capacity)
// and returns to the pool.
// The buffer must not be used after this call.
func (p *MemPool) ReleaseBuffer(buf *bytes.Buffer) {
	// Clear before releasing
	// but keep the allocated capacity
	buf.Reset()
	p.pool.Put(buf)
}
