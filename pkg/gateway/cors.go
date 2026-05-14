package gateway

import (
	"net/http"
	"strings"
)

// explorerCORSMiddleware adds CORS for read-only public explorer endpoints so a
// browser SPA (e.g. Cloudflare Pages) can call the gateway API cross-origin.
// allowedOrigins must be exact Origin header values (scheme + host + optional port).
func explorerCORSMiddleware(allowedOrigins []string, next http.Handler) http.Handler {
	allowed := make(map[string]struct{}, len(allowedOrigins))
	for _, o := range allowedOrigins {
		o = strings.TrimSpace(o)
		if o == "" {
			continue
		}
		allowed[o] = struct{}{}
	}
	if len(allowed) == 0 {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !explorerCORSPath(r.URL.Path) {
			next.ServeHTTP(w, r)
			return
		}
		origin := strings.TrimSpace(r.Header.Get("Origin"))
		if origin == "" {
			next.ServeHTTP(w, r)
			return
		}
		if _, ok := allowed[origin]; !ok {
			next.ServeHTTP(w, r)
			return
		}
		w.Header().Set("Vary", "Origin")
		w.Header().Set("Access-Control-Allow-Origin", origin)
		if r.Method == http.MethodOptions {
			w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
			w.Header().Set("Access-Control-Max-Age", "86400")
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func explorerCORSPath(path string) bool {
	switch path {
	case "/health", "/v1/models", "/v1/network/nodes":
		return true
	default:
		return false
	}
}
