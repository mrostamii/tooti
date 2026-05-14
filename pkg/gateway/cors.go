package gateway

import (
	"net"
	"net/http"
	"net/url"
	"strings"
)

// explorerCORSMiddleware adds CORS for read-only public explorer endpoints so a
// browser SPA (e.g. Cloudflare Pages) can call the gateway API cross-origin.
// allowedOrigins must be exact Origin header values (scheme + host + optional port).
func explorerCORSMiddleware(allowedOrigins []string, next http.Handler) http.Handler {
	allowed := make(map[string]struct{}, len(allowedOrigins))
	for _, o := range allowedOrigins {
		normalized := normalizeOrigin(o)
		if normalized == "" {
			continue
		}
		allowed[normalized] = struct{}{}
		for _, alias := range loopbackAliases(normalized) {
			allowed[alias] = struct{}{}
		}
	}
	if len(allowed) == 0 {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !explorerCORSPath(r.URL.Path) {
			next.ServeHTTP(w, r)
			return
		}
		origin := normalizeOrigin(r.Header.Get("Origin"))
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

func normalizeOrigin(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	if u.Scheme == "" || u.Host == "" {
		return ""
	}
	host := strings.ToLower(strings.TrimSpace(u.Host))
	scheme := strings.ToLower(strings.TrimSpace(u.Scheme))
	return scheme + "://" + host
}

func loopbackAliases(origin string) []string {
	u, err := url.Parse(origin)
	if err != nil || u.Host == "" {
		return nil
	}
	host := u.Hostname()
	if host == "" {
		return nil
	}
	port := u.Port()
	switch host {
	case "localhost", "127.0.0.1", "::1":
	default:
		return nil
	}
	candidates := []string{"localhost", "127.0.0.1", "::1"}
	out := make([]string, 0, len(candidates))
	for _, c := range candidates {
		h := c
		if port != "" {
			h = net.JoinHostPort(h, port)
		} else if strings.Contains(h, ":") {
			h = "[" + h + "]"
		}
		out = append(out, u.Scheme+"://"+h)
	}
	return out
}
