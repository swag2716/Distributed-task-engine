package localserver

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
)

func SpinLocalServer() *httptest.Server {
	// ── local test server so we dont depend on network ──────────
	var callCount atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count := callCount.Add(1)
		fmt.Printf("  [server] request #%d → %s\n", count, r.URL.Path)

		switch r.URL.Path {
		case "/success":
			w.WriteHeader(200)
		case "/fail":
			w.WriteHeader(500) // always fails
		case "/flaky":
			if count <= 2 {
				w.WriteHeader(500) // fails first 2 times
			} else {
				w.WriteHeader(200) // succeeds on 3rd
			}
		}
	}))

	fmt.Printf("local server at %s\n\n", server.URL)
	return server
}
