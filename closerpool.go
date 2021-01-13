package sarama

import "io"

// closer2 support for Close() that don't return error
type closer2 interface {
	Close()
}

// CloserPool is provide pool functionalities (similar to sync.pool)
// for items that need to be Close() after use
type CloserPool struct {
	items chan interface{}
	newFn func() interface{}
}

// NewCloserPool creates a new CloserPool
// capacity specifies the max number of items stored
// in the pool. If more items are put, these will be
// Closed and dropped.
// newFn optionally specifies a function to generate
// a value when Get would otherwise return nil.
// CloserPool should be Closed after use.
func NewCloserPool(capacity int, newFn func() interface{}) *CloserPool {
	return &CloserPool{
		newFn: newFn,
		items: make(chan interface{}, capacity),
	}
}

// Close closes all pool items
func (p *CloserPool) Close() (err error) {
	if p.items == nil {
		return nil
	}
	for {
		select {
		case i := <-p.items:
			if c, ok := i.(io.Closer); ok {
				if errClose := c.Close(); errClose != nil {
					err = errClose
				}
			} else if c, ok := i.(closer2); ok {
				c.Close()
			}
		default:
			close(p.items)
			p.items = nil
			p.newFn = nil
			return nil
		}
	}
}

// Get gets an item from the pool.
// If no item is available and a newCb has been
// provided, an item will be created with that Callback.
// Otherwise nil is returned.
// The caller of Get() may put the item back (not Closed) in the pool using Put()
func (p *CloserPool) Get() interface{} {
	select {
	case res := <-p.items:
		return res
	default:
		if p.newFn == nil {
			return nil
		}
		return p.newFn()
	}
}

// Put puts an item back in the pool.
// If the pool is full, the item is Closed and dropped
func (p *CloserPool) Put(i interface{}) {
	select {
	case p.items <- i:
	default:
		// items if full, just Close and drop
		// the pool item
		if c, ok := i.(io.Closer); ok {
			c.Close()
		} else if c, ok := i.(closer2); ok {
			c.Close()
		}
	}
}
