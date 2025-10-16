package pymk

import (
	"container/heap"
	"math"
	"sync"
	"time"

	"github.com/yourname/social-graph/internal/embeds"
	"github.com/yourname/social-graph/internal/graph"
	"github.com/yourname/social-graph/internal/metrics"
)

// -------- Utilities --------
func intersectCount(a, b map[uint64]struct{}, capAt int) (cnt int) {
	// iterate smaller
	if len(a) > len(b) {
		a, b = b, a
	}
	for x := range a {
		if _, ok := b[x]; ok {
			cnt++
			if capAt > 0 && cnt >= capAt {
				return
			}
		}
	}
	return
}
func unionSize(a, b map[uint64]struct{}) int {
	inter := intersectCount(a, b, 0)
	return len(a) + len(b) - inter
}
func toStdSet(s graph.Store, ids []uint64) map[uint64]struct{} {
	if len(ids) == 0 { return nil }
	m := make(map[uint64]struct{}, len(ids))
	for _, id := range ids { m[id] = struct{}{} }
	return m
}

func cosine(a, b []float32) float64 {
	if len(a) == 0 || len(a) != len(b) { return 0 }
	var dot, na, nb float64
	for i := range a {
		av := float64(a[i])
		bv := float64(b[i])
		dot += av * bv
		na  += av * av
		nb  += bv * bv
	}
	if na == 0 || nb == 0 { return 0 }
	res := dot / (math.Sqrt(na) * math.Sqrt(nb))
	if res < 0 { return 0 } // clamp negatives if desired
	return res
}

// -------- Public types --------
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
	MaxExpandPerNeighbor int
	MaxCandidates        int
	WCommon              float64
	WJaccard             float64
	WAA                  float64
	WCosine              float64
	CacheSize            int
	CacheTTL             time.Duration
}

type Service struct {
	G graph.Store
	E embeds.Store
	C PYMKConfig

	cacheMu sync.RWMutex
	cache   *lruCache
}

func NewService(g graph.Store, e embeds.Store, cfg PYMKConfig) *Service {
	s := &Service{G: g, E: e, C: cfg}
	s.cache = newLRU(cfg.CacheSize, cfg.CacheTTL)
	s.cache.onHit  = func(){ metrics.PYMKCache.WithLabelValues("hit").Inc() }
	s.cache.onMiss = func(){ metrics.PYMKCache.WithLabelValues("miss").Inc() }
	s.cache.onEvict= func(){ metrics.PYMKCache.WithLabelValues("evict").Inc() }
	return s
}

// Stats per candidate while expanding
type candStats struct {
	common int
	aa     float64
}

type scored struct {
	id       uint64
	common   int
	jaccard  float64
	aa       float64
	cos      float64
	score    float64
}

// The core PYMK algorithm with caching & fan-out caps.
func (s *Service) PYMK(u uint64, k int, exclude map[uint64]struct{}) []Suggestion {
	if k <= 0 { k = 20 }
	epoch := s.G.UserEpoch(u)

	// 0) Cache
	key := cacheKey{user: u, k: k, epoch: epoch}
	if got, ok := s.cache.Get(key); ok {
		return got
	}

	// 1) One-hop sets
	outU := toStdSet(s.G, s.G.Following(u))
	inU  := toStdSet(s.G, s.G.Followers(u))

	oneHop := make(map[uint64]struct{}, len(outU)+len(inU))
	for x := range outU { oneHop[x] = struct{}{} }
	for x := range inU  { oneHop[x] = struct{}{} }

	// 2) Expand two-hop
	stats := make(map[uint64]*candStats, 1024)
	expand := func(src map[uint64]struct{}) {
		for n := range src {
			neighbors := s.G.Following(n) // bias: outgoing neighbors
			if s.C.MaxExpandPerNeighbor > 0 && len(neighbors) > s.C.MaxExpandPerNeighbor {
				neighbors = neighbors[:s.C.MaxExpandPerNeighbor]
			}
			degN := s.G.DegreeOut(n) + s.G.DegreeIn(n)
			aaWeight := 0.0
			if degN > 0 {
				aaWeight = 1.0 / math.Log(float64(1+degN)+1e-9)
			}
			for _, c := range neighbors {
				if c == u { continue }
				if _, ok := oneHop[c]; ok { continue }
				if exclude != nil {
					if _, bad := exclude[c]; bad { continue }
				}
				cs := stats[c]
				if cs == nil {
					cs = &candStats{}
					stats[c] = cs
				}
				cs.common++
				cs.aa += aaWeight
				if s.C.MaxCandidates > 0 && len(stats) >= s.C.MaxCandidates {
					// soft cap; keep accumulating for existing keys
				}
			}
		}
	}
	expand(outU)
	expand(inU)

	if len(stats) == 0 {
		s.cache.Set(key, []Suggestion{})
		return []Suggestion{}
	}

	// 3) Compute features for each candidate
	degU := len(outU)
	var uvec []float32
	if s.E != nil {
		if v, ok := s.E.Get(u); ok { uvec = v }
	}

	var (
		maxCommon int
		maxJacc   float64
		maxAA     float64
		maxCos    float64
	)
	out := make([]scored, 0, len(stats))
	for id, st := range stats {
		outC := toStdSet(s.G, s.G.Following(id))
		jacc := 0.0
		if degU > 0 || len(outC) > 0 {
			jacc = float64(intersectCount(outU, outC, 0)) / (float64(unionSize(outU, outC)) + 1e-9)
		}
		cos := 0.0
		if uvec != nil && s.E != nil {
			if v, ok := s.E.Get(id); ok {
				cos = cosine(uvec, v)
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

	// 4) Weighted scoring with min-max normalization
	for i := range out {
		var nCommon, nJ, nAA, nCos float64
		if maxCommon > 0 { nCommon = float64(out[i].common) / float64(maxCommon) }
		if maxJacc   > 0 { nJ = out[i].jaccard / maxJacc }
		if maxAA     > 0 { nAA = out[i].aa / maxAA }
		if maxCos    > 0 { nCos = out[i].cos / maxCos }
		out[i].score = s.C.WCommon*nCommon + s.C.WJaccard*nJ + s.C.WAA*nAA + s.C.WCosine*nCos
	}

	// 5) Top-K via min-heap
	h := &minHeap{}; heap.Init(h)
	for i := range out {
		if h.Len() < k {
			heap.Push(h, out[i])
		} else if out[i].score > (*h)[0].score {
			heap.Pop(h)
			heap.Push(h, out[i])
		}
	}

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

	// 6) Cache & return
	s.cache.Set(key, res)
	return res
}

// -------- Heap for Top-K --------
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
