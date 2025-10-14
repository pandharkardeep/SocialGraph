package main

import (
	"container/heap"
	"encoding/json"
	"log"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

type scored struct {
    id       uint64
    common   int
    jaccard  float64
    aa       float64
    cos      float64
    score    float64
}

// ---------- Types & utils ----------

type void struct{}

var member void

type uint64Set map[uint64]void

func (s uint64Set) Add(x uint64) { s[x] = member }
func (s uint64Set) Del(x uint64) { delete(s, x) }
func (s uint64Set) Has(x uint64) bool {
	_, ok := s[x]; return ok
}
func (s uint64Set) Len() int { return len(s) }

func intersectCount(a, b uint64Set, capAt int) (cnt int) {
	// iterate smaller set
	if len(a) > len(b) {
		a, b = b, a
	}
	for x := range a {
		if b.Has(x) {
			cnt++
			if capAt > 0 && cnt >= capAt {
				return
			}
		}
	}
	return
}

func unionSize(a, b uint64Set) int {
	// |A| + |B| - |A∩B|
	inter := intersectCount(a, b, 0)
	return len(a) + len(b) - inter
}

// ---------- Graph Store (sharded) ----------

const shards = 64

type shard struct {
	mu        sync.RWMutex
	following map[uint64]uint64Set // u -> set(dst)
	followers map[uint64]uint64Set // v -> set(src)
}

type GraphStore interface {
	Follow(u, v uint64) bool
	Unfollow(u, v uint64) bool
	Following(u uint64) []uint64
	Followers(u uint64) []uint64
	HasEdge(u, v uint64) bool
	DegreeOut(u uint64) int
	DegreeIn(u uint64) int
}

type MemGraph struct {
	ss [shards]*shard
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

	// lock order by shard index to avoid deadlock
	a, b := su, sv
	if su != sv && h(u) > h(v) {
		a, b = sv, su
	}
	a.mu.Lock()
	if b != a { b.mu.Lock() }

	// upsert
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
	return true
}

func (g *MemGraph) Unfollow(u, v uint64) bool {
	su := g.ss[h(u)]
	sv := g.ss[h(v)]
	a, b := su, sv
	if su != sv && h(u) > h(v) {
		a, b = sv, su
	}
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
		return true
	}

	if b != a { b.mu.Unlock() }
	a.mu.Unlock()
	return false
}

func (g *MemGraph) Following(u uint64) []uint64 {
	s := g.ss[h(u)]
	s.mu.RLock()
	defer s.mu.RUnlock()
	fset := s.following[u]
	out := make([]uint64, 0, len(fset))
	for v := range fset { out = append(out, v) }
	return out
}

func (g *MemGraph) Followers(u uint64) []uint64 {
	s := g.ss[h(u)]
	s.mu.RLock()
	defer s.mu.RUnlock()
	rset := s.followers[u]
	out := make([]uint64, 0, len(rset))
	for v := range rset { out = append(out, v) }
	return out
}

func (g *MemGraph) HasEdge(u, v uint64) bool {
	s := g.ss[h(u)]
	s.mu.RLock()
	defer s.mu.RUnlock()
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

// ---------- Embedding Store ----------

type EmbedStore interface {
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

func cosine(a, b []float32) float64 {
	if len(a) == 0 || len(a) != len(b) { return 0 }
	var dot, na, nb float64
	for i := range a {
		av := float64(a[i])
		bv := float64(b[i])
		dot += av * bv
		na += av * av
		nb += bv * bv
	}
	if na == 0 || nb == 0 { return 0 }
	return dot / (math.Sqrt(na) * math.Sqrt(nb))
}

// ---------- PYMK Service ----------

type Suggestion struct {
	UserID uint64  `json:"user_id"`
	Score  float64 `json:"score"`
	Why    struct {
		CommonNeighbors int     `json:"common_neighbors"`
		Jaccard         float64 `json:"jaccard"`
		AdamicAdar      float64 `json:"adamic_adar"`
		Cosine          float64 `json:"cosine"`
	} `json:"why"`
}

type PYMKConfig struct {
	MaxExpandPerNeighbor int     // cap second-hop expansion per neighbor
	MaxCandidates        int     // cap total raw candidates
	WCommon              float64 // weights
	WJaccard             float64
	WAA                  float64
	WCosine              float64
}

type Service struct {
	G GraphStore
	E EmbedStore
	C PYMKConfig
}

func NewService(g GraphStore, e EmbedStore) *Service {
	return &Service{
		G: g,
		E: e,
		C: PYMKConfig{
			MaxExpandPerNeighbor: 200,
			MaxCandidates:        20000,
			WCommon:              1.0,
			WJaccard:             0.6,
			WAA:                  0.8,
			WCosine:              1.0,
		},
	}
}

type candStats struct {
	common int
	aa     float64
}

func (s *Service) PYMK(u uint64, k int, exclude map[uint64]void) []Suggestion {
	if k <= 0 { k = 20 }

	// 1) One-hop neighborhood
	followingU := toSet(s.G.Following(u))
	followersU := toSet(s.G.Followers(u))
	oneHop := make(uint64Set, followingU.Len()+followersU.Len())
	for x := range followingU { oneHop.Add(x) }
	for x := range followersU { oneHop.Add(x) }

	// 2) Expand two-hop candidates from following (primary) and optionally followers
	stats := make(map[uint64]*candStats, 1024)

	expandFrom := func(src uint64Set) {
		for n := range src {
			neighbors := s.G.Following(n) // use outgoing for recommendation bias
			if len(neighbors) > s.C.MaxExpandPerNeighbor {
				neighbors = neighbors[:s.C.MaxExpandPerNeighbor]
			}
			degN := s.G.DegreeOut(n) + s.G.DegreeIn(n)
			aaWeight := 0.0
			if degN > 0 {
				aaWeight = 1.0 / math.Log(float64(1+degN)+1e-9)
			}
			for _, c := range neighbors {
				if c == u { continue }
				if oneHop.Has(c) { continue } // already connected somehow
				if exclude != nil {
					if _, bad := exclude[c]; bad { continue }
				}
				cs := stats[c]
				if cs == nil {
					cs = &candStats{}
					stats[c] = cs
				}
				cs.common++          // proxy for shared neighbor via n
				cs.aa += aaWeight    // Adamic-Adar accumulation
				if len(stats) >= s.C.MaxCandidates {
					// soft cap; we'll still score existing ones
				}
			}
		}
	}

	expandFrom(followingU)
	expandFrom(followersU)

	if len(stats) == 0 {
		return []Suggestion{}
	}

	// 3) Compute feature scores
	type scored struct {
		id       uint64
		common   int
		jaccard  float64
		aa       float64
		cos      float64
		score    float64
	}
	out := make([]scored, 0, len(stats))
	// pre-fetch u's sets for Jaccard
	outU := followingU
	if outU == nil { outU = uint64Set{} }
	inU := followersU
	if inU == nil { inU = uint64Set{} }

	// We'll consider jaccard over OUT neighbors primarily; feel free to blend with IN.
	degU := outU.Len()

	// pre-load u embedding
	var uvec []float32
	if s.E != nil {
		if v, ok := s.E.Get(u); ok {
			uvec = v
		}
	}

	// track for normalization
	var maxCommon int
	var maxJacc, maxAA, maxCos float64

	for id, st := range stats {
		// Jaccard over outgoing sets (following)
		outC := toSet(s.G.Following(id))
		jacc := 0.0
		if degU > 0 || outC.Len() > 0 {
			jacc = float64(intersectCount(outU, outC, 0)) / (float64(unionSize(outU, outC)) + 1e-9)
		}

		// Cosine
		cos := 0.0
		if uvec != nil && s.E != nil {
			if v, ok := s.E.Get(id); ok {
				cos = cosine(uvec, v)
				if cos < 0 { cos = 0 } // clamp negatives if desired
			}
		}

		sc := scored{
			id:      id,
			common:  st.common,
			jaccard: jacc,
			aa:      st.aa,
			cos:     cos,
		}
		if sc.common > maxCommon { maxCommon = sc.common }
		if sc.jaccard > maxJacc { maxJacc = sc.jaccard }
		if sc.aa > maxAA { maxAA = sc.aa }
		if sc.cos > maxCos { maxCos = sc.cos }
		out = append(out, sc)
	}

	// 4) Weighted sum with simple min-max normalization (per request)
	for i := range out {
		var nCommon, nJ, nAA, nCos float64
		if maxCommon > 0 { nCommon = float64(out[i].common) / float64(maxCommon) }
		if maxJacc   > 0 { nJ = out[i].jaccard / maxJacc }
		if maxAA     > 0 { nAA = out[i].aa / maxAA }
		if maxCos    > 0 { nCos = out[i].cos / maxCos }

		out[i].score = s.C.WCommon*nCommon + s.C.WJaccard*nJ + s.C.WAA*nAA + s.C.WCosine*nCos
	}

	// 5) Top-K
	h := &minHeap{}
	heap.Init(h)
	for i := range out {
		if h.Len() < k {
			heap.Push(h, out[i])
		} else if out[i].score > (*h)[0].score {
			heap.Pop(h)
			heap.Push(h, out[i])
		}
	}

	// materialize descending by score
	res := make([]Suggestion, h.Len())
	for i := len(res)-1; i >= 0; i-- {
		it := heap.Pop(h).(scored)
		sug := Suggestion{UserID: it.id, Score: it.score}
		sug.Why.CommonNeighbors = it.common
		sug.Why.Jaccard = it.jaccard
		sug.Why.AdamicAdar = it.aa
		sug.Why.Cosine = it.cos
		res[i] = sug
	}
	return res
}

func toSet(list []uint64) uint64Set {
	if len(list) == 0 { return nil }
	s := make(uint64Set, len(list))
	for _, x := range list { s.Add(x) }
	return s
}

// ---------- min-heap for top-K ----------

type minHeap []scored
func (h minHeap) Len() int            { return len(h) }
func (h minHeap) Less(i, j int) bool  { return h[i].score < h[j].score }
func (h minHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *minHeap) Push(x interface{}) { *h = append(*h, x.(scored)) }
func (h *minHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// ---------- HTTP Handlers ----------

type server struct {
	svc *Service
}

func (s *server) parseID(q string) (uint64, error) {
	return strconv.ParseUint(q, 10, 64)
}

func (s *server) postFollow(w http.ResponseWriter, r *http.Request) {
	type req struct{ Src, Dst uint64 }
	var body req
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), 400); return
	}
	ok := s.svc.G.Follow(body.Src, body.Dst)
	writeJSON(w, map[string]any{"ok": ok})
}

func (s *server) postUnfollow(w http.ResponseWriter, r *http.Request) {
	type req struct{ Src, Dst uint64 }
	var body req
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), 400); return
	}
	ok := s.svc.G.Unfollow(body.Src, body.Dst)
	writeJSON(w, map[string]any{"ok": ok})
}

func (s *server) getFollowing(w http.ResponseWriter, r *http.Request) {
	u, err := s.parseID(r.URL.Query().Get("user_id"))
	if err != nil { http.Error(w, "bad user_id", 400); return }
	writeJSON(w, s.svc.G.Following(u))
}
func (s *server) getFollowers(w http.ResponseWriter, r *http.Request) {
	u, err := s.parseID(r.URL.Query().Get("user_id"))
	if err != nil { http.Error(w, "bad user_id", 400); return }
	writeJSON(w, s.svc.G.Followers(u))
}
func (s *server) getMutuals(w http.ResponseWriter, r *http.Request) {
	u, err1 := s.parseID(r.URL.Query().Get("u"))
	v, err2 := s.parseID(r.URL.Query().Get("v"))
	if err1 != nil || err2 != nil { http.Error(w, "bad ids", 400); return }
	uf := toSet(s.svc.G.Following(u))
	vf := toSet(s.svc.G.Following(v))
	if uf == nil || vf == nil {
		writeJSON(w, []uint64{}); return
	}
	res := make([]uint64, 0, 8)
	// intersect
	if uf.Len() > vf.Len() { uf, vf = vf, uf }
	for x := range uf { if vf.Has(x) { res = append(res, x) } }
	writeJSON(w, res)
}

func (s *server) putEmbedding(w http.ResponseWriter, r *http.Request) {
	type req struct {
		UserID uint64    `json:"user_id"`
		Vec    []float32 `json:"vector"`
	}
	var body req
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), 400); return
	}
	if len(body.Vec) == 0 { http.Error(w, "empty vector", 400); return }
	s.svc.E.Put(body.UserID, body.Vec)
	writeJSON(w, map[string]any{"ok": true})
}

func (s *server) getPYMK(w http.ResponseWriter, r *http.Request) {
	u, err := s.parseID(r.URL.Query().Get("user_id"))
	if err != nil { http.Error(w, "bad user_id", 400); return }
	k := 20
	if q := strings.TrimSpace(r.URL.Query().Get("k")); q != "" {
		if v, err := strconv.Atoi(q); err == nil && v > 0 { k = v }
	}
	// Example: exclude list via query (?exclude=1,2,3)
	ex := make(map[uint64]void)
	if exStr := strings.TrimSpace(r.URL.Query().Get("exclude")); exStr != "" {
		parts := strings.Split(exStr, ",")
		for _, p := range parts {
			if id, err := strconv.ParseUint(strings.TrimSpace(p), 10, 64); err == nil {
				ex[id] = member
			}
		}
	}
	res := s.svc.PYMK(u, k, ex)
	writeJSON(w, res)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func main() {
	g := NewMemGraph()
	e := NewMemEmbeds()
	svc := NewService(g, e)
	s := &server{svc: svc}

	mux := http.NewServeMux()
	mux.HandleFunc("/follow", s.postFollow)       // POST
	mux.HandleFunc("/unfollow", s.postUnfollow)   // POST
	mux.HandleFunc("/following", s.getFollowing)  // GET
	mux.HandleFunc("/followers", s.getFollowers)  // GET
	mux.HandleFunc("/mutuals", s.getMutuals)      // GET
	mux.HandleFunc("/embedding", s.putEmbedding)  // PUT
	mux.HandleFunc("/pymk", s.getPYMK)            // GET

	srv := &http.Server{
		Addr:              ":8080",
		Handler:           logging(mux),
		ReadHeaderTimeout: 5 * time.Second,
	}
	log.Println("graph service listening on :8080")
	log.Fatal(srv.ListenAndServe())
}

func logging(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request){
		start := time.Now()
		h.ServeHTTP(w, r)
		log.Printf("%s %s %v", r.Method, r.URL.Path, time.Since(start))
	})
}
