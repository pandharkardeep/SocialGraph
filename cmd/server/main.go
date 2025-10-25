package main

import (
	"log"
	"net/http"
	"os"
	"time"
	"github.com/pandharkardeep/social-graph/internal/embeds"
	"github.com/pandharkardeep/social-graph/internal/graph"
	"github.com/pandharkardeep/social-graph/internal/metrics"
	"github.com/pandharkardeep/social-graph/internal/pymk"
	"github.com/pandharkardeep/social-graph/internal/server"
)

func main() {
	// --- Core stores ---
	g := graph.NewMemGraph()
	e := embeds.NewMemEmbeds()

	// --- PYMK service with sensible defaults ---
	svc := pymk.NewService(g, e, pymk.PYMKConfig{
		MaxExpandPerNeighbor: 200,   // fan-out cap per neighbor
		MaxCandidates:        20000, // hard-ish cap
		WCommon:              1.00,
		WJaccard:             0.60,
		WAA:                  0.80,
		WCosine:              1.00,
		CacheSize:            100_000,        // LRU entries
		CacheTTL:             2 * time.Minute, // short TTL to stay fresh
	})

	// --- HTTP server & routes ---
	mux := http.NewServeMux()
	server.AttachRoutes(mux, svc, g, e)

	addr := getenv("ADDR", ":8080")
	srv := &http.Server{
		Addr:              addr,
		Handler:           metrics.HTTPMetricsMiddleware(mux),
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Printf("social-graph listening on %s", addr)
	log.Fatal(srv.ListenAndServe())
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
