package controlplane

import (
	"context"
	"net/http"
	"os"
	"testing"
)

// Serves the pages against the same fakes the tests use, so the UI can be iterated on without a
// cluster or an image. Skipped unless an address is named:
//
//	NEON_CTL_UI_DEV=:8081 go test ./internal/controlplane -run UIDev -count=1 -timeout 0
func TestUIDev(t *testing.T) {
	addr := os.Getenv("NEON_CTL_UI_DEV")
	if addr == "" {
		t.Skip("set NEON_CTL_UI_DEV to an address to serve the pages")
	}

	store := newStore(t)
	branch := seedBranch(t, store)
	// Keyed, and with a password actually kept, so the connect string has one to unmask.
	server := passwordServer(t, store)
	server.opts.EndpointSuffix = "pg.example.net"
	sealed, err := server.seal(branch.Roles[0].Name, "correct-horse-battery-staple")
	if err != nil {
		t.Fatal(err)
	}
	branch.Roles[0].Secret = sealed
	if err := store.Put(context.Background(), branch); err != nil {
		t.Fatal(err)
	}

	user := os.Getenv("NEON_CTL_UI_DEV_USER")
	if user == "" {
		user = "tester"
	}
	groups := os.Getenv("NEON_CTL_UI_DEV_GROUPS")

	handler := server.Handler()
	t.Logf("serving as %q on http://localhost%s", user, addr)
	// The proxy that decides this is not in front of us here.
	t.Fatal(http.ListenAndServe(addr, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Header.Set("Remote-User", user)
		if groups != "" {
			r.Header.Set("Remote-Groups", groups)
		}
		handler.ServeHTTP(w, r)
	})))
}
