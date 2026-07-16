package inmem

import (
	"context"
	"net/http"
	"testing"

	s3 "github.com/fil-forge/libforge/commands/s3"
	"github.com/fil-forge/libforge/commands/s3/bucket"
	"github.com/fil-forge/libforge/testutil"
)

// TestMemStoreListBucketsPagination covers the local pagination semantics of
// the BucketAuthority ListBuckets seam: prefix filter, resume-after-token, max
// truncation (with the token set to the last included name), and the empty
// token on a complete listing. The options ride as query parameters on the
// (signed) request URL, which is what MemStore reads.
func TestMemStoreListBucketsPagination(t *testing.T) {
	ctx := context.Background()
	m := NewMemStore()
	for _, name := range []string{"apple", "apricot", "banana", "cherry"} {
		space := testutil.RandomDID(t)
		if err := m.Create(ctx, name, space); err != nil {
			t.Fatalf("Create %q: %v", name, err)
		}
	}

	names := func(p *bucket.ListOK) []string {
		out := make([]string, 0, len(p.Buckets))
		for _, b := range p.Buckets {
			out = append(out, b.Name)
		}
		return out
	}
	equal := func(a, b []string) bool {
		if len(a) != len(b) {
			return false
		}
		for i := range a {
			if a[i] != b[i] {
				return false
			}
		}
		return true
	}

	// listReq builds a ListBuckets request whose options ride as query
	// parameters on the URL (MemStore parses them straight off req.URL).
	listReq := func(query string) s3.Request {
		return s3.Request{Method: http.MethodGet, URL: "/" + query}
	}

	t.Run("full listing, no cap", func(t *testing.T) {
		page, err := m.ListBuckets(ctx, listReq(""))
		if err != nil {
			t.Fatalf("ListBuckets: %v", err)
		}
		if got := names(page); !equal(got, []string{"apple", "apricot", "banana", "cherry"}) {
			t.Fatalf("buckets = %v", got)
		}
		if page.ContinuationToken != "" {
			t.Fatalf("token = %q, want empty on complete listing", page.ContinuationToken)
		}
	})

	t.Run("prefix filter", func(t *testing.T) {
		page, err := m.ListBuckets(ctx, listReq("?prefix=ap"))
		if err != nil {
			t.Fatalf("ListBuckets: %v", err)
		}
		if got := names(page); !equal(got, []string{"apple", "apricot"}) {
			t.Fatalf("buckets = %v", got)
		}
	})

	t.Run("max truncates and sets token", func(t *testing.T) {
		page, err := m.ListBuckets(ctx, listReq("?max-buckets=2"))
		if err != nil {
			t.Fatalf("ListBuckets: %v", err)
		}
		if got := names(page); !equal(got, []string{"apple", "apricot"}) {
			t.Fatalf("buckets = %v", got)
		}
		if page.ContinuationToken != "apricot" {
			t.Fatalf("token = %q, want %q", page.ContinuationToken, "apricot")
		}
	})

	t.Run("token resumes strictly after", func(t *testing.T) {
		page, err := m.ListBuckets(ctx, listReq("?continuation-token=apricot&max-buckets=2"))
		if err != nil {
			t.Fatalf("ListBuckets: %v", err)
		}
		if got := names(page); !equal(got, []string{"banana", "cherry"}) {
			t.Fatalf("buckets = %v", got)
		}
		if page.ContinuationToken != "" {
			t.Fatalf("token = %q, want empty on final page", page.ContinuationToken)
		}
	})
}
