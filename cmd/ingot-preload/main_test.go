package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/fil-forge/ingot/internal/preload"
)

// TestLoadCommand runs `load` against an endpoint that accepts every
// request, and checks what it leaves behind: the state, the metrics and the
// progress lines.
func TestLoadCommand(t *testing.T) {
	none := filepath.Join(t.TempDir(), "none")
	t.Setenv("AWS_CONFIG_FILE", none)
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", none)
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_ENDPOINT_URL", "")
	t.Setenv("AWS_ACCESS_KEY_ID", "test-access-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret-key")

	var mu sync.Mutex
	puts := map[string]int{}
	hs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		puts[r.Method+" "+strings.SplitN(strings.TrimPrefix(r.URL.Path, "/"), "/", 2)[0]]++
		mu.Unlock()
	}))
	defer hs.Close()

	dir := t.TempDir()
	state := filepath.Join(dir, "state.json")
	metrics := filepath.Join(dir, "metrics.jsonl")
	var out bytes.Buffer
	cmd := newRootCmd()
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"load",
		"--endpoint", hs.URL, "--region", "us-east-1",
		"--buckets", "2", "--until", "7",
		"--state", state, "--metrics", metrics,
	})
	if err := cmd.ExecuteContext(t.Context()); err != nil {
		t.Fatal(err)
	}

	// Two bucket creates, two encryption settings, seven objects each.
	if puts["PUT preload-000"] != 9 || puts["PUT preload-001"] != 9 {
		t.Fatalf("requests %v", puts)
	}
	st, err := preload.ReadState(state, "preload", "")
	if err != nil {
		t.Fatal(err)
	}
	if b := st.Buckets["preload-001"]; b == nil || !b.Ready || b.Bulk != 7 {
		t.Fatalf("state %+v", st.Buckets)
	}
	raw, err := os.ReadFile(metrics)
	if err != nil {
		t.Fatal(err)
	}
	var last preload.Report
	if err := json.Unmarshal(bytes.TrimSpace(raw), &last); err != nil {
		t.Fatalf("metrics %q: %v", raw, err)
	}
	if !last.Final || last.Total.Written != 14 {
		t.Fatalf("final report %+v", last)
	}
	if !strings.Contains(out.String(), "14 written") || !strings.Contains(out.String(), "(final)") {
		t.Fatalf("progress %q", out.String())
	}
}
