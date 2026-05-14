package gateway

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestExplorerCORSMiddleware(t *testing.T) {
	t.Parallel()
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	})
	h := explorerCORSMiddleware([]string{"https://tooti.network", "http://localhost:5173/"}, next)

	t.Run("OPTIONS allowed origin", func(t *testing.T) {
		t.Parallel()
		req := httptest.NewRequest(http.MethodOptions, "/v1/models", nil)
		req.Header.Set("Origin", "https://tooti.network")
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		res := rr.Result()
		defer res.Body.Close()
		if res.StatusCode != http.StatusNoContent {
			t.Fatalf("status=%d", res.StatusCode)
		}
		if got := res.Header.Get("Access-Control-Allow-Origin"); got != "https://tooti.network" {
			t.Fatalf("ACAO=%q", got)
		}
		if got := res.Header.Get("Access-Control-Allow-Methods"); got != "GET, OPTIONS" {
			t.Fatalf("Allow-Methods=%q", got)
		}
	})

	t.Run("GET allowed origin passes through with headers", func(t *testing.T) {
		t.Parallel()
		req := httptest.NewRequest(http.MethodGet, "/v1/network/nodes", nil)
		req.Header.Set("Origin", "http://localhost:5173")
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		res := rr.Result()
		defer res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("status=%d", res.StatusCode)
		}
		if got := res.Header.Get("Access-Control-Allow-Origin"); got != "http://localhost:5173" {
			t.Fatalf("ACAO=%q", got)
		}
	})

	t.Run("loopback alias allows localhost and 127.0.0.1", func(t *testing.T) {
		t.Parallel()
		req := httptest.NewRequest(http.MethodGet, "/v1/network/nodes", nil)
		req.Header.Set("Origin", "http://127.0.0.1:5173")
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		res := rr.Result()
		defer res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("status=%d", res.StatusCode)
		}
		if got := res.Header.Get("Access-Control-Allow-Origin"); got != "http://127.0.0.1:5173" {
			t.Fatalf("ACAO=%q", got)
		}
	})

	t.Run("GET disallowed origin no ACAO", func(t *testing.T) {
		t.Parallel()
		req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
		req.Header.Set("Origin", "https://evil.example")
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		res := rr.Result()
		defer res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("status=%d", res.StatusCode)
		}
		if got := res.Header.Get("Access-Control-Allow-Origin"); got != "" {
			t.Fatalf("unexpected ACAO=%q", got)
		}
	})

	t.Run("non explorer path bypasses CORS logic for disallowed", func(t *testing.T) {
		t.Parallel()
		req := httptest.NewRequest(http.MethodGet, "/v1/chat/completions", nil)
		req.Header.Set("Origin", "https://tooti.network")
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		res := rr.Result()
		defer res.Body.Close()
		if got := res.Header.Get("Access-Control-Allow-Origin"); got != "" {
			t.Fatalf("unexpected ACAO on non-explorer path: %q", got)
		}
	})
}

func TestExplorerCORSMiddlewareEmptyAllowlist(t *testing.T) {
	t.Parallel()
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	})
	h := explorerCORSMiddleware(nil, next)
	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	req.Header.Set("Origin", "https://tooti.network")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusTeapot {
		t.Fatalf("status=%d", rr.Code)
	}
}
