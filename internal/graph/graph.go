package graph

import "sync"

// -------- Basic set --------
type void struct{}
var member void

type uint64Set map[uint64]void
func (s uint64Set) Add(x uint64)   { s[x] = member }
func (s uint64Set) Del(x uint64)   { delete(s, x) }
func (s uint64Set) Has(x uint64) bool { _, ok := s[x]; return ok }
func (s uint64Set) Len() int       { return len(s) }

func ToSet(list []uint64) uint64Set {
	if len(list) == 0 { return nil }
	s := make(uint64Set, len(list))
	for _, x := range list { s.Add(x) }
	return s
}

// -------- Graph interface --------
type Store interface {
	Follow(u, v uint64) bool
	Unfollow(u, v uint64) bool
	Following(u uint64) []uint64
	Followers(u uint64) []uint64
	HasEdge(u, v uint64) bool
	DegreeOut(u uint64) int
	DegreeIn(u uint64) int
	TouchUsers(users ...uint64) // increments users' epoch for cache invalidation
	UserEpoch(u uint64) uint64
}

// -------- Sharded in-memory graph --------
const shards = 64

type shard struct {
	mu        sync.RWMutex
	following map[uint64]uint64Set // u -> set(dst)
	followers map[uint64]uint64Set // v -> set(src)
}

type MemGraph struct {
	ss     [shards]*shard
	epochs sync.Map // user -> uint64 epoch for cache invalidation
}

func NewMemGraph() *MemGraph {
	g := &MemGraph{}
	for i := 0; i < shards; i++ {
		g.ss[i] = &shard{
			following: make(map[uint64]uint64Set),
			followers: make(map[uint64]uint64Set),
		}
	}
	return g
}

func h(u uint64) int { return int(u % shards) }

func (g *MemGraph) Follow(u, v uint64) bool {
	if u == v { return false }
	su := g.ss[h(u)]
	sv := g.ss[h(v)]

	// Lock order by shard index to avoid deadlock.
	a, b := su, sv
	if su != sv && h(u) > h(v) { a, b = sv, su }
	a.mu.Lock()
	if b != a { b.mu.Lock() }

	fset, ok := su.following[u]
	if !ok {
		fset = make(uint64Set)
		su.following[u] = fset
	}
	if fset.Has(v) {
		if b != a { b.mu.Unlock() }
		a.mu.Unlock()
		return false
	}
	fset.Add(v)

	rset, ok := sv.followers[v]
	if !ok {
		rset = make(uint64Set)
		sv.followers[v] = rset
	}
	rset.Add(u)

	if b != a { b.mu.Unlock() }
	a.mu.Unlock()

	g.TouchUsers(u, v)
	return true
}

func (g *MemGraph) Unfollow(u, v uint64) bool {
	su := g.ss[h(u)]
	sv := g.ss[h(v)]
	a, b := su, sv
	if su != sv && h(u) > h(v) { a, b = sv, su }
	a.mu.Lock()
	if b != a { b.mu.Lock() }

	fset, ok := su.following[u]
	if ok && fset.Has(v) {
		fset.Del(v)
		if len(fset) == 0 {
			delete(su.following, u)
		}
		if rset, ok := sv.followers[v]; ok {
			rset.Del(u)
			if len(rset) == 0 {
				delete(sv.followers, v)
			}
		}
		if b != a { b.mu.Unlock() }
		a.mu.Unlock()

		g.TouchUsers(u, v)
		return true
	}

	if b != a { b.mu.Unlock() }
	a.mu.Unlock()
	return false
}

func (g *MemGraph) Following(u uint64) []uint64 {
	s := g.ss[h(u)]
	s.mu.RLock(); defer s.mu.RUnlock()
	fset := s.following[u]
	out := make([]uint64, 0, len(fset))
	for v := range fset { out = append(out, v) }
	return out
}

func (g *MemGraph) Followers(u uint64) []uint64 {
	s := g.ss[h(u)]
	s.mu.RLock(); defer s.mu.RUnlock()
	rset := s.followers[u]
	out := make([]uint64, 0, len(rset))
	for v := range rset { out = append(out, v) }
	return out
}

func (g *MemGraph) HasEdge(u, v uint64) bool {
	s := g.ss[h(u)]
	s.mu.RLock(); defer s.mu.RUnlock()
	if fset, ok := s.following[u]; ok {
		return fset.Has(v)
	}
	return false
}
func (g *MemGraph) DegreeOut(u uint64) int {
	s := g.ss[h(u)]
	s.mu.RLock(); defer s.mu.RUnlock()
	return len(s.following[u])
}
func (g *MemGraph) DegreeIn(u uint64) int {
	s := g.ss[h(u)]
	s.mu.RLock(); defer s.mu.RUnlock()
	return len(s.followers[u])
}

// Cache invalidation epochs per user
func (g *MemGraph) TouchUsers(users ...uint64) {
	for _, u := range users {
		cur := g.UserEpoch(u)
		g.epochs.Store(u, cur+1)
	}
}
func (g *MemGraph) UserEpoch(u uint64) uint64 {
	if v, ok := g.epochs.Load(u); ok {
		return v.(uint64)
	}
	return 0
}
