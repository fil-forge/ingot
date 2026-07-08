package inmem

import (
	"context"
	"testing"

	"github.com/fil-forge/ingot/registry"
)

// TestMemStoreListPagination covers the local pagination semantics: prefix
// filter, resume-after-token, max truncation (with the token set to the last
// included name), and the empty token on a complete listing.
func TestMemStoreListPagination(t *testing.T) {
	ctx := context.Background()
	m := NewMemStore()
	for _, name := range []string{"apple", "apricot", "banana", "cherry"} {
		if err := m.Create(ctx, name); err != nil {
			t.Fatalf("Create %q: %v", name, err)
		}
	}

	names := func(p *registry.Page) []string {
		out := make([]string, 0, len(p.Buckets))
		for _, st := range p.Buckets {
			out = append(out, st.Name)
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

	t.Run("full listing, no cap", func(t *testing.T) {
		page, err := m.List(ctx, registry.ListOptions{})
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if got := names(page); !equal(got, []string{"apple", "apricot", "banana", "cherry"}) {
			t.Fatalf("buckets = %v", got)
		}
		if page.ContinuationToken != "" {
			t.Fatalf("token = %q, want empty on complete listing", page.ContinuationToken)
		}
	})

	t.Run("prefix filter", func(t *testing.T) {
		page, err := m.List(ctx, registry.ListOptions{Prefix: "ap"})
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if got := names(page); !equal(got, []string{"apple", "apricot"}) {
			t.Fatalf("buckets = %v", got)
		}
	})

	t.Run("max truncates and sets token", func(t *testing.T) {
		page, err := m.List(ctx, registry.ListOptions{Max: 2})
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if got := names(page); !equal(got, []string{"apple", "apricot"}) {
			t.Fatalf("buckets = %v", got)
		}
		if page.ContinuationToken != "apricot" {
			t.Fatalf("token = %q, want %q", page.ContinuationToken, "apricot")
		}
	})

	t.Run("token resumes strictly after", func(t *testing.T) {
		page, err := m.List(ctx, registry.ListOptions{ContinuationToken: "apricot", Max: 2})
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if got := names(page); !equal(got, []string{"banana", "cherry"}) {
			t.Fatalf("buckets = %v", got)
		}
		if page.ContinuationToken != "" {
			t.Fatalf("token = %q, want empty on final page", page.ContinuationToken)
		}
	})
}
