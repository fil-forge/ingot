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

// isolateAWS points the SDK at no shared config and placeholder credentials.
func isolateAWS(t *testing.T) {
	t.Helper()
	none := filepath.Join(t.TempDir(), "none")
	t.Setenv("AWS_CONFIG_FILE", none)
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", none)
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_ENDPOINT_URL", "")
	t.Setenv("AWS_ACCESS_KEY_ID", "test-access-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret-key")
}

// TestLoadCommand runs `load` against an endpoint that accepts every
// request, and checks what it leaves behind: the state, the metrics and the
// progress lines.
func TestLoadCommand(t *testing.T) {
	isolateAWS(t)

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

// keyServer keeps the keys PUT to it, empty: enough for a load and for
// validate's head and write steps. A HEAD of a key in missing finds nothing.
type keyServer struct {
	mu      sync.Mutex
	keys    map[string]bool
	missing map[string]bool
}

func (s *keyServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	path := strings.TrimPrefix(r.URL.Path, "/")
	if !strings.Contains(path, "/") {
		return // a bucket's creation or encryption
	}
	switch r.Method {
	case http.MethodPut:
		s.keys[path] = true
	case http.MethodDelete:
		delete(s.keys, path)
	case http.MethodHead:
		if !s.keys[path] || s.missing[path] {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Length", "0")
	}
}

// TestValidateCommand loads a bucket and validates it, once finding every
// object and once missing one.
func TestValidateCommand(t *testing.T) {
	isolateAWS(t)
	srv := &keyServer{keys: map[string]bool{}, missing: map[string]bool{}}
	hs := httptest.NewServer(srv)
	defer hs.Close()
	dir := t.TempDir()
	state := filepath.Join(dir, "state.json")
	report := filepath.Join(dir, "validate.jsonl")
	run := func(args ...string) (string, error) {
		var out bytes.Buffer
		cmd := newRootCmd()
		cmd.SetOut(&out)
		cmd.SetArgs(append(args, "--endpoint", hs.URL, "--region", "us-east-1", "--buckets", "1", "--state", state))
		err := cmd.ExecuteContext(t.Context())
		return out.String(), err
	}
	if _, err := run("load", "--until", "6", "--metrics", ""); err != nil {
		t.Fatal(err)
	}
	validate := func(label string) (string, error) {
		return run("validate", "--label", label, "--steps", "head,write", "--samples", "6", "--report", report)
	}
	if out, err := validate("all-there"); err != nil || !strings.Contains(out, "steps finished: head,write. Buckets: 1, holding 6 to 6 bulk objects each. Failed or wrong: 0.") {
		t.Fatalf("validate: %v\n%s", err, out)
	}
	srv.mu.Lock()
	srv.missing["preload-000/"+preload.BulkKey("", "preload", "preload-000", 4)] = true
	srv.mu.Unlock()
	out, err := validate("one-missing")
	if err == nil || !strings.Contains(out, "Failed or wrong: 1.") || !strings.Contains(out, "StatusCode: 404") {
		t.Fatalf("validate with a missing object: %v\n%s", err, out)
	}

	raw, err := os.ReadFile(report)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(lines) != 2 {
		t.Fatalf("report %q", raw)
	}
	var last preload.ValidateReport
	if err := json.Unmarshal([]byte(lines[1]), &last); err != nil {
		t.Fatal(err)
	}
	if last.Label != "one-missing" || last.Failures != 1 || last.Latency[preload.OpPut].Count != 6 {
		t.Fatalf("report %+v", last)
	}
	if len(srv.keys) != 6 {
		t.Fatalf("%d keys left, want the load's 6", len(srv.keys))
	}
}
