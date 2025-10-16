package embeds

import "sync"

type Store interface {
	Get(user uint64) ([]float32, bool)
	Put(user uint64, vec []float32)
}

type MemEmbeds struct {
	mu  sync.RWMutex
	vec map[uint64][]float32
}

func NewMemEmbeds() *MemEmbeds { return &MemEmbeds{vec: make(map[uint64][]float32)} }

func (e *MemEmbeds) Get(user uint64) ([]float32, bool) {
	e.mu.RLock(); defer e.mu.RUnlock()
	v, ok := e.vec[user]; return v, ok
}
func (e *MemEmbeds) Put(user uint64, vec []float32) {
	e.mu.Lock(); defer e.mu.Unlock()
	e.vec[user] = vec
}
