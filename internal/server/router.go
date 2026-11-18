package server

import (
	"net/http"
	"strings"

	"gitforge/internal/githttp"
	"gitforge/internal/graphql"
	"gitforge/internal/mcp"
	"gitforge/internal/rest"
	"gitforge/internal/store"
)

var (
	healthzBody  = []byte(`{"ok":true}` + "\n")
	notFoundBody = []byte(`{"message":"Not Found"}` + "\n")
	jsonCT       = []string{"application/json; charset=utf-8"}
)

// Router dispatches incoming requests across /healthz, REST v3, GraphQL v4, MCP, and Git Smart HTTP.
type Router struct {
	Forge   *store.Forge
	Rest    *rest.Handler
	GraphQL *graphql.Handler
	MCP     *mcp.Handler
	Git     *githttp.Handler
}

func NewRouter(f *store.Forge) *Router {
	return &Router{
		Forge:   f,
		Rest:    rest.NewHandler(f),
		GraphQL: graphql.NewHandler(f),
		MCP:     mcp.NewHandler(f),
		Git:     githttp.NewHandler(f),
	}
}

func (rt *Router) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p := r.URL.Path

	// Fast path 1: REST API v3 (/api/v3 and /api/v3/...)
	if strings.HasPrefix(p, "/api/v3") && (len(p) == 7 || p[7] == '/') {
		rt.Rest.ServeHTTP(w, r)
		return
	}

	// Fast path 2: Boot probe GET /healthz (no auth required per wire.ts)
	if p == "/healthz" && r.Method == http.MethodGet {
		hdr := w.Header()
		hdr["Content-Type"] = jsonCT
		hdr["X-Github-Request-Id"] = rt.Forge.NextRequestIDSlice()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(healthzBody)
		return
	}

	// Fast path 3: GraphQL v4
	if p == "/api/graphql" {
		rt.GraphQL.ServeHTTP(w, r)
		return
	}

	// Fast path 4: MCP Streamable HTTP
	if p == "/mcp" {
		rt.MCP.ServeHTTP(w, r)
		return
	}

	// Fast path 5: Git Smart HTTP (/{owner}/{repo}.git/...)
	if rt.Git.TryServe(w, r) {
		return
	}

	// Everything else: GitHub JSON 404
	hdr := w.Header()
	hdr["Content-Type"] = jsonCT
	hdr["X-Github-Request-Id"] = rt.Forge.NextRequestIDSlice()
	w.WriteHeader(http.StatusNotFound)
	_, _ = w.Write(notFoundBody)
}
