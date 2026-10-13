package proxy

import "sync"

const DefaultBufferSize = 32 * 1024 // 32 KB standard socket transfer chunk

// BufferPool manages a pool of pre-allocated byte slices to eliminate per-copy allocations.
type BufferPool struct {
	pool sync.Pool
	size int
}

// NewBufferPool initializes a pool with fixed buffer capacities.
func NewBufferPool(size int) *BufferPool {
	if size <= 0 {
		size = DefaultBufferSize
	}
	return &BufferPool{
		size: size,
		pool: sync.Pool{
			New: func() any {
				b := make([]byte, size)
				return &b
			},
		},
	}
}

// Get acquires a byte slice pointer from the pool.
func (p *BufferPool) Get() *[]byte {
	return p.pool.Get().(*[]byte)
}

// Put returns the byte slice pointer back into the pool.
func (p *BufferPool) Put(b *[]byte) {
	if b == nil || cap(*b) < p.size {
		return
	}
	// Reset slice length to original allocation size before returning
	*b = (*b)[:p.size]
	p.pool.Put(b)
}