package metrics

import (
	"net/http"
	"time"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	RequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "sg_requests_total",
			Help: "Total HTTP requests by method and path.",
		},
		[]string{"method", "path"},
	)
	RequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "sg_request_duration_seconds",
			Help:    "HTTP request duration in seconds by method and path.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "path"},
	)
	FollowOps = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "sg_follow_ops_total",
			Help: "Follow/Unfollow operations.",
		},
		[]string{"op"}, // follow | unfollow
	)
	PYMKCache = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "sg_pymk_cache_events_total",
			Help: "PYMK cache events.",
		},
		[]string{"event"}, // hit | miss | evict
	)
)

func init() {
	prometheus.MustRegister(RequestsTotal, RequestDuration, FollowOps, PYMKCache)
}

func Handler() http.Handler { return promhttp.Handler() }

func HTTPMetricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		path := r.URL.Path
		RequestsTotal.WithLabelValues(r.Method, path).Inc()
		next.ServeHTTP(w, r)
		RequestDuration.WithLabelValues(r.Method, path).Observe(time.Since(start).Seconds())
	})
}
