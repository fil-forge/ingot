package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/fil-forge/ingot/internal/preload"
	"github.com/stretchr/testify/require"
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
	require.NoError(t, cmd.ExecuteContext(t.Context()))

	// Two bucket creates, two encryption settings, seven objects each.
	require.Equal(t, 9, puts["PUT preload-000"], "requests %v", puts)
	require.Equal(t, 9, puts["PUT preload-001"], "requests %v", puts)
	st, err := preload.ReadState(state, "preload", "")
	require.NoError(t, err)
	b := st.Buckets["preload-001"]
	require.NotNil(t, b, "state %+v", st.Buckets)
	require.True(t, b.Ready, "state %+v", st.Buckets)
	require.Equal(t, uint64(7), b.Bulk, "state %+v", st.Buckets)
	raw, err := os.ReadFile(metrics)
	require.NoError(t, err)
	var last preload.Report
	require.NoError(t, json.Unmarshal(bytes.TrimSpace(raw), &last), "metrics %q", raw)
	require.True(t, last.Final, "final report %+v", last)
	require.Equal(t, uint64(14), last.Total.Written, "final report %+v", last)
	require.Contains(t, out.String(), "14 written", "progress")
	require.Contains(t, out.String(), "(final)", "progress")
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
		_, _ = io.Copy(io.Discard, r.Body)
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
	_, err := run("load", "--until", "6", "--metrics", "")
	require.NoError(t, err)
	validate := func(label string) (string, error) {
		return run("validate", "--label", label, "--steps", "head,write", "--samples", "6",
			"--write-real", "2", "--real-min-mib", "1", "--real-max-mib", "1", "--report", report)
	}
	out, err := validate("all-there")
	require.NoError(t, err, "validate:\n%s", out)
	require.Contains(t, out, "steps finished: head,write. Buckets: 1, holding 6 to 6 bulk objects each. Failed or wrong: 0.", "validate")
	srv.mu.Lock()
	srv.missing["preload-000/"+preload.BulkKey("", "preload", "preload-000", 4)] = true
	srv.mu.Unlock()
	out, err = validate("one-missing")
	require.Error(t, err, "validate with a missing object: %v\n%s", err, out)
	require.Contains(t, out, "Failed or wrong: 1.", "validate with a missing object: %v\n%s", err, out)
	require.Contains(t, out, "StatusCode: 404", "validate with a missing object: %v\n%s", err, out)

	raw, err := os.ReadFile(report)
	require.NoError(t, err)
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	require.Len(t, lines, 2, "report %q", raw)
	var last preload.ValidateReport
	require.NoError(t, json.Unmarshal([]byte(lines[1]), &last))
	require.Equal(t, "one-missing", last.Label, "report %+v", last)
	require.Equal(t, uint64(1), last.Failures, "report %+v", last)
	require.Equal(t, uint64(6), last.Latency[preload.OpPut].Count, "report %+v", last)
	require.Equal(t, uint64(2), last.Latency[preload.OpPutReal].Count, "report %+v", last)
	require.Equal(t, uint64(2), last.Latency[preload.OpDeleteReal].Count, "report %+v", last)
	require.Len(t, srv.keys, 6, "%d keys left", len(srv.keys))
}
