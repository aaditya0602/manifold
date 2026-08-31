package proxy

import "sync"

// copyBufferSize matches the size httputil.ReverseProxy uses when no
// BufferPool is supplied, so pooling changes allocation behaviour without
// changing how much is read per syscall.
const copyBufferSize = 32 * 1024

// bufferPool supplies the buffers ReverseProxy uses to copy response bodies
// from upstream to client.
//
// With a nil BufferPool -- the default, and what manifold shipped through
// Week 3 -- ReverseProxy allocates a fresh 32KB buffer for *every* proxied
// response. At the 13.7k req/s this proxy sustains, that is roughly 450 MB/s
// of garbage produced for no reason, and it showed up exactly where you would
// expect: a CPU profile under load attributed 8-10% of samples to the garbage
// collector (scanObjectsSmall, scanblock, tryDeferToSpanScan,
// memclrNoHeapPointers), and the request benchmark reported 39KB allocated per
// request against response bodies of about a hundred bytes.
//
// The buffers are pooled per process rather than per pool: they hold no
// per-pool state, and sharing them means a burst on one pool warms the cache
// for the others.
//
// Put is deliberately tolerant of a wrong-sized buffer. ReverseProxy always
// returns what it was given, but a pool that silently accepted a short buffer
// would produce truncated copies far from here, and one that panicked would
// turn a library change into an outage. Anything unexpected is dropped on the
// floor and re-allocated next time.
type bufferPool struct{ pool sync.Pool }

func newBufferPool() *bufferPool {
	return &bufferPool{
		pool: sync.Pool{
			New: func() any {
				b := make([]byte, copyBufferSize)
				return &b
			},
		},
	}
}

// Get implements httputil.BufferPool.
func (b *bufferPool) Get() []byte { return *(b.pool.Get().(*[]byte)) }

// Put implements httputil.BufferPool.
func (b *bufferPool) Put(buf []byte) {
	if cap(buf) != copyBufferSize {
		return
	}
	buf = buf[:copyBufferSize]
	b.pool.Put(&buf)
}
