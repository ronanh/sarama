package sarama

import (
	"testing"
)

type closer struct {
	close func() error
}

func (c *closer) Close() error {
	return c.close()
}
func TestCloserPool(t *testing.T) {
	p := NewCloserPool(16, nil)

	if p.Get() != nil {
		t.Fatal("expected empty")
	}

	var closeCount int
	c := closer{
		close: func() error {
			closeCount++
			return nil
		},
	}
	p.Put(&c)
	p.Put(&c)
	if g := p.Get(); g != &c {
		t.Fatalf("got %p; want %p", g, &c)
	}
	if g := p.Get(); g != &c {
		t.Fatalf("got %p; want %p", g, &c)
	}
	if g := p.Get(); g != nil {
		t.Fatalf("got %p; want nil", g)
	}
	p.Put(&c)
	p.Put(&c)
	p.Put(&c)
	if closeCount != 0 {
		t.Fatalf("got %v; want 0", closeCount)
	}
	p.Close()
	if closeCount != 3 {
		t.Fatalf("got %v; want 3", closeCount)
	}

}

func TestCloserPoolNew(t *testing.T) {
	var closeCount int
	p := NewCloserPool(8, func() interface{} {
		return &closer{
			close: func() error {
				closeCount++
				return nil
			},
		}
	})

	var rscs []interface{}
	for i := 0; i < 12; i++ {
		rscs = append(rscs, p.Get())
	}
	if closeCount != 0 {
		t.Fatalf("got %d; want 0", closeCount)
	}
	for _, r := range rscs {
		p.Put(r)
	}
	// Capacity is 8, we put 12, so 4 should be closed
	if closeCount != 4 {
		t.Fatalf("got %d; want 4", closeCount)
	}
	p.Close()
	// Pool closed, all resources should be closed
	if closeCount != 12 {
		t.Fatalf("got %d; want 12", closeCount)
	}
}
