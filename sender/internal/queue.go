package internal

type Queue struct {
	pool *MemPool
}

func NewQueue(messageSize int, capacity int) *Queue {
	pool := NewMemPool(messageSize)
	return &Queue{
		pool: pool,
	}
}
