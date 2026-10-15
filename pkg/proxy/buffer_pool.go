package proxy

import "sync"

const DefaultBufferSize = 32 * 1024 // 32 KB

// BufferPool manages pre-allocated byte slices to achieve zero-allocation transfers.
type BufferPool struct {
	pool sync.Pool
	size int
}

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

func (p *BufferPool) Get() *[]byte {
	return p.pool.Get().(*[]byte)
}

func (p *BufferPool) Put(b *[]byte) {
	if b == nil || cap(*b) < p.size {
		return
	}
	*b = (*b)[:p.size]
	p.pool.Put(b)
}
