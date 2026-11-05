package pgtest

import "testing"

// TestWithDatabase: the database named replaces the DSN's own, wherever the
// DSN names it, and other options are kept.
func TestWithDatabase(t *testing.T) {
	for dsn, want := range map[string]string{
		"postgres://u:p@h:5432/postgres?sslmode=disable":         "postgres://u:p@h:5432/clone?sslmode=disable",
		"postgres://u:p@h:5432/?dbname=postgres&sslmode=disable": "postgres://u:p@h:5432/clone?sslmode=disable",
		"host=h user=u dbname=postgres":                          "host=h user=u dbname=postgres dbname=clone",
	} {
		got, err := withDatabase(dsn, "clone")
		if err != nil || got != want {
			t.Errorf("withDatabase(%q) = %q, %v; want %q", dsn, got, err, want)
		}
	}
}
