# Social Graph (Go)

Low-latency social-graph microservice with sharded in-memory adjacency, O(1) follow/unfollow, “People You May Know” (2-hop + Common Neighbors, Jaccard, Adamic–Adar, cosine), caching, fan-out caps, Prometheus metrics, and k6 load tests.

## Run locally

```bash
go mod tidy
make run
# or
go run ./cmd/server
```
