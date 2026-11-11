package preload

import (
	"crypto/sha256"
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// isolateAWSConfig points the SDK at no shared config and placeholder
// credentials, so a test sees nothing of the machine it runs on.
func isolateAWSConfig(t *testing.T) {
	t.Helper()
	none := filepath.Join(t.TempDir(), "none")
	t.Setenv("AWS_CONFIG_FILE", none)
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", none)
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_REGION", "")
	t.Setenv("AWS_DEFAULT_REGION", "")
	t.Setenv("AWS_ENDPOINT_URL", "")
	t.Setenv("AWS_ACCESS_KEY_ID", "test-access-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret-key")
}

// packServerShape is an S3 endpoint that fails the test unless each PUT has
// the shape the pack server's: path-style, create-only, with the body's
// SHA-256 and no other checksum.
type packServerShape struct {
	t          *testing.T
	mu         sync.Mutex
	bucket     string
	created    bool
	encryption string
	objects    map[string]bool
	// intercept, when set, sees each object PUT first; returning true
	// means it has answered the request (or dropped it).
	intercept func(key string, w http.ResponseWriter, r *http.Request) bool
}

func (s *packServerShape) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if prefix := "/" + s.bucket + "/"; s.intercept != nil && r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, prefix) {
		if s.intercept(strings.TrimPrefix(r.URL.Path, prefix), w, r) {
			return
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	body, err := io.ReadAll(r.Body)
	if err != nil {
		s.t.Errorf("read body: %v", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	bucketPath := "/" + s.bucket
	switch {
	case r.Method == http.MethodPut && r.URL.Path == bucketPath && r.URL.Query().Has("encryption"):
		s.encryption = string(body)
	case r.Method == http.MethodPut && r.URL.Path == bucketPath:
		if s.created {
			writeS3Error(w, http.StatusConflict, "BucketAlreadyOwnedByYou")
			return
		}
		s.created = true
	case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, bucketPath+"/"):
		key := strings.TrimPrefix(r.URL.Path, bucketPath+"/")
		if got := r.Header.Get("If-None-Match"); got != "*" {
			s.t.Errorf("PUT %s: If-None-Match %q, want *", key, got)
		}
		sum := sha256.Sum256(body)
		if got, want := r.Header.Get("X-Amz-Checksum-Sha256"), base64.StdEncoding.EncodeToString(sum[:]); got != want {
			s.t.Errorf("PUT %s: x-amz-checksum-sha256 %q, want %q", key, got, want)
		}
		for name := range r.Header {
			lower := strings.ToLower(name)
			if (strings.HasPrefix(lower, "x-amz-checksum-") && lower != "x-amz-checksum-sha256") || lower == "x-amz-trailer" {
				s.t.Errorf("PUT %s: unexpected header %s", key, name)
			}
		}
		if s.objects[key] {
			writeS3Error(w, http.StatusPreconditionFailed, "PreconditionFailed")
			return
		}
		s.objects[key] = true
		w.Header().Set("ETag", `"etag"`)
	default:
		s.t.Errorf("unexpected request %s %s", r.Method, r.URL)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func writeS3Error(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	io.WriteString(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>`+code+`</Code><Message>m</Message></Error>`)
}

func TestClientSendsThePackServersRequests(t *testing.T) {
	isolateAWSConfig(t)
	cfg := testConfig(t, 1, 5)
	cfg.KeyPrefix = "site-a/"
	cfg.RealEvery = 2
	cfg.RealMinSize, cfg.RealMaxSize = 3<<20, 3<<20
	srv := &packServerShape{t: t, bucket: cfg.Buckets[0], objects: map[string]bool{
		BulkKey(cfg.KeyPrefix, cfg.Seed, cfg.Buckets[0], 1): true,
	}}
	hs := httptest.NewServer(srv)
	defer hs.Close()

	client, err := NewClient(t.Context(), ClientConfig{Endpoint: hs.URL, Region: "us-east-1", Conns: 4})
	if err != nil {
		t.Fatal(err)
	}
	r, err := Load(t.Context(), client, cfg)
	if err != nil {
		t.Fatal(err)
	}
	// Five bulk and two real objects, one bulk key already taken.
	if r.Total.Written != 6 || r.Total.Existed != 1 || r.Total.Real != 2 {
		t.Fatalf("totals %+v", r.Total)
	}
	if !strings.Contains(srv.encryption, "<SSEAlgorithm>none</SSEAlgorithm>") {
		t.Fatalf("encryption request %q does not set none", srv.encryption)
	}

	// A load from a fresh state finds the bucket its own and every key taken.
	cfg.StatePath = filepath.Join(t.TempDir(), "state.json")
	r, err = Load(t.Context(), client, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if r.Total.Written != 0 || r.Total.Existed != 7 {
		t.Fatalf("second load totals %+v", r.Total)
	}
}

// testAttemptTimeout bounds each PUT attempt in the tests that cut one off.
// Only the attempt being cut off comes near it: a retry, even of a real
// object's body under -race on a busy machine, takes a fraction of it.
const testAttemptTimeout = 2 * time.Second

// TestClientRetriesWhenNoResponseComes runs a load through the real SDK
// client while the server, once, gives a PUT no response: it holds the
// request past the attempt's timeout, or drops the connection. Either way
// the PUT is retried and the load completes.
func TestClientRetriesWhenNoResponseComes(t *testing.T) {
	for name, noResponse := range map[string]func(w http.ResponseWriter, r *http.Request){
		"attempt times out": func(_ http.ResponseWriter, r *http.Request) {
			io.Copy(io.Discard, r.Body)
			<-r.Context().Done()
		},
		"connection dropped": func(w http.ResponseWriter, _ *http.Request) {
			conn, _, err := w.(http.Hijacker).Hijack()
			if err == nil {
				conn.Close()
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			isolateAWSConfig(t)
			cfg := testConfig(t, 1, 5)
			cfg.AttemptTimeout = testAttemptTimeout
			victim := BulkKey(cfg.KeyPrefix, cfg.Seed, cfg.Buckets[0], 2)
			var once sync.Once
			srv := &packServerShape{t: t, bucket: cfg.Buckets[0], objects: map[string]bool{}}
			srv.intercept = func(key string, w http.ResponseWriter, r *http.Request) bool {
				hit := false
				if key == victim {
					once.Do(func() { hit = true })
				}
				if hit {
					noResponse(w, r)
				}
				return hit
			}
			hs := httptest.NewServer(srv)
			defer hs.Close()
			client, err := NewClient(t.Context(), ClientConfig{Endpoint: hs.URL, Region: "us-east-1", Conns: 4})
			if err != nil {
				t.Fatal(err)
			}
			r, err := Load(t.Context(), client, cfg)
			if err != nil {
				t.Fatal(err)
			}
			if r.Total.Errors != 1 || r.Total.Written != 5 {
				t.Fatalf("totals %+v, want the PUT with no response retried once", r.Total)
			}
		})
	}
}

// TestClientResendsACutOffRealBody cuts off a real object's PUT part way
// through its body, by holding it until the attempt times out or by dropping
// the connection. The retry must send the whole body again from its start,
// which the server checks against the PUT's SHA-256.
func TestClientResendsACutOffRealBody(t *testing.T) {
	const cutAt = 1<<20 + 17
	// release ends the held attempt's handler once the subtest is done: a
	// server notices a client leave only once it has read the whole body,
	// so the handler can't wait for that, and the client's timeout alone
	// decides when the attempt is cut off.
	for name, cutOff := range map[string]func(release <-chan struct{}, w http.ResponseWriter, r *http.Request){
		"attempt times out": func(release <-chan struct{}, _ http.ResponseWriter, r *http.Request) {
			io.CopyN(io.Discard, r.Body, cutAt)
			<-release
			io.Copy(io.Discard, r.Body)
		},
		"connection dropped": func(_ <-chan struct{}, w http.ResponseWriter, r *http.Request) {
			io.CopyN(io.Discard, r.Body, cutAt)
			conn, _, err := w.(http.Hijacker).Hijack()
			if err == nil {
				conn.Close()
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			isolateAWSConfig(t)
			cfg := testConfig(t, 1, 5)
			cfg.AttemptTimeout = testAttemptTimeout
			cfg.RealEvery = 5
			cfg.RealMinSize, cfg.RealMaxSize = 3<<20+5, 3<<20+5
			release := make(chan struct{})
			var once sync.Once
			srv := &packServerShape{t: t, bucket: cfg.Buckets[0], objects: map[string]bool{}}
			srv.intercept = func(_ string, w http.ResponseWriter, r *http.Request) bool {
				hit := false
				if r.ContentLength > 0 {
					once.Do(func() { hit = true })
				}
				if hit {
					cutOff(release, w, r)
				}
				return hit
			}
			hs := httptest.NewServer(srv)
			defer hs.Close()
			// Deferred after Close, so it runs first on every path, a panic
			// included: Close waits for the held handler to return.
			defer close(release)
			client, err := NewClient(t.Context(), ClientConfig{Endpoint: hs.URL, Region: "us-east-1", Conns: 4})
			if err != nil {
				t.Fatal(err)
			}
			r, err := Load(t.Context(), client, cfg)
			if err != nil {
				t.Fatal(err)
			}
			if r.Total.Errors != 1 || r.Total.Real != 1 || r.Total.Written != 6 {
				t.Fatalf("totals %+v, want the cut-off real PUT sent again once", r.Total)
			}
		})
	}
}

func TestNewClientNeedsRegionAndEndpoint(t *testing.T) {
	isolateAWSConfig(t)
	if _, err := NewClient(t.Context(), ClientConfig{Endpoint: "http://127.0.0.1:1"}); err == nil || !strings.Contains(err.Error(), "region") {
		t.Fatalf("no region: %v", err)
	}
	if _, err := NewClient(t.Context(), ClientConfig{Region: "us-east-1"}); err == nil || !strings.Contains(err.Error(), "endpoint") {
		t.Fatalf("no endpoint: %v", err)
	}
}
