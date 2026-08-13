package internal

import "sync"

type MemPool struct {
	pool *sync.Pool
}

func NewMemPool(bufferSize int) *MemPool {
	return &MemPool{
		pool: &sync.Pool{
			New: func() any {
				return make([]byte, 0, bufferSize)
			},
		},
	}
}
