package server

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/yourname/social-graph/internal/embeds"
	"github.com/yourname/social-graph/internal/graph"
	"github.com/yourname/social-graph/internal/metrics"
	"github.com/yourname/social-graph/internal/pymk"
)

type server struct {
	svc *pymk.Service
	g   graph.Store
	e   embeds.Store
}

func AttachRoutes(mux *http.ServeMux, svc *pymk.Service, g graph.Store, e embeds.Store) {
	s := &server{svc: svc, g: g, e: e}

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte("ok"))
	})
	mux.Handle("/metrics", metrics.Handler())

	mux.HandleFunc("/follow", s.postFollow)       // POST
	mux.HandleFunc("/unfollow", s.postUnfollow)   // POST
	mux.HandleFunc("/following", s.getFollowing)  // GET
	mux.HandleFunc("/followers", s.getFollowers)  // GET
	mux.HandleFunc("/mutuals", s.getMutuals)      // GET
	mux.HandleFunc("/embedding", s.putEmbedding)  // PUT
	mux.HandleFunc("/pymk", s.getPYMK)            // GET
}

func (s *server) parseID(q string) (uint64, error) {
	return strconv.ParseUint(q, 10, 64)
}

func (s *server) postFollow(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost { http.Error(w, "method not allowed", 405); return }
	type req struct{ Src, Dst uint64 }
	var body req
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), 400); return
	}
	ok := s.g.Follow(body.Src, body.Dst)
	if ok { metrics.FollowOps.WithLabelValues("follow").Inc() }
	writeJSON(w, map[string]any{"ok": ok})
}

func (s *server) postUnfollow(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost { http.Error(w, "method not allowed", 405); return }
	type req struct{ Src, Dst uint64 }
	var body req
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), 400); return
	}
	ok := s.g.Unfollow(body.Src, body.Dst)
	if ok { metrics.FollowOps.WithLabelValues("unfollow").Inc() }
	writeJSON(w, map[string]any{"ok": ok})
}

func (s *server) getFollowing(w http.ResponseWriter, r *http.Request) {
	u, err := s.parseID(r.URL.Query().Get("user_id"))
	if err != nil { http.Error(w, "bad user_id", 400); return }
	writeJSON(w, s.g.Following(u))
}
func (s *server) getFollowers(w http.ResponseWriter, r *http.Request) {
	u, err := s.parseID(r.URL.Query().Get("user_id"))
	if err != nil { http.Error(w, "bad user_id", 400); return }
	writeJSON(w, s.g.Followers(u))
}
func (s *server) getMutuals(w http.ResponseWriter, r *http.Request) {
	u, err1 := s.parseID(r.URL.Query().Get("u"))
	v, err2 := s.parseID(r.URL.Query().Get("v"))
	if err1 != nil || err2 != nil { http.Error(w, "bad ids", 400); return }
	uf := graph.ToSet(s.g.Following(u))
	vf := graph.ToSet(s.g.Following(v))
	if uf == nil || vf == nil {
		writeJSON(w, []uint64{}); return
	}
	res := make([]uint64, 0, 8)
	if uf.Len() > vf.Len() { uf, vf = vf, uf }
	for x := range uf { if vf.Has(x) { res = append(res, x) } }
	writeJSON(w, res)
}

func (s *server) putEmbedding(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut { http.Error(w, "method not allowed", 405); return }
	type req struct {
		UserID uint64    `json:"user_id"`
		Vec    []float32 `json:"vector"`
	}
	var body req
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), 400); return
	}
	if len(body.Vec) == 0 { http.Error(w, "empty vector", 400); return }
	s.e.Put(body.UserID, body.Vec)
	writeJSON(w, map[string]any{"ok": true})
}

func (s *server) getPYMK(w http.ResponseWriter, r *http.Request) {
	u, err := s.parseID(r.URL.Query().Get("user_id"))
	if err != nil { http.Error(w, "bad user_id", 400); return }
	k := 20
	if q := strings.TrimSpace(r.URL.Query().Get("k")); q != "" {
		if v, err := strconv.Atoi(q); err == nil && v > 0 { k = v }
	}
	// ?exclude=1,2,3
	var ex map[uint64]struct{}
	if exStr := strings.TrimSpace(r.URL.Query().Get("exclude")); exStr != "" {
		ex = make(map[uint64]struct{})
		for _, p := range strings.Split(exStr, ",") {
			if id, err := strconv.ParseUint(strings.TrimSpace(p), 10, 64); err == nil {
				ex[id] = struct{}{}
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
