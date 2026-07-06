package cmd

import (
	"strings"
	"testing"

	"github.com/fil-forge/ingot"
)

// validStandaloneConfig is a minimal DaemonConfig that passes Validate.
func validStandaloneConfig() DaemonConfig {
	return DaemonConfig{
		Config: ingot.Config{
			Addr:       "127.0.0.1:9000",
			DataDir:    "/data",
			RootAccess: "key",
			RootSecret: "secret",
		},
		Mode: ModeStandalone,
	}
}

// TestValidate_Hilt covers the optional hilt block: unset is fine, a full
// url+did pair is fine, and any partial configuration is rejected.
func TestValidate_Hilt(t *testing.T) {
	cases := []struct {
		name    string
		url     string
		did     string
		proofs  string
		wantErr string
	}{
		{name: "unset"},
		{name: "url and did", url: "http://127.0.0.1:8080", did: "did:web:hilt.example"},
		{name: "url only", url: "http://127.0.0.1:8080", wantErr: "hilt_url and hilt_did"},
		{name: "did only", did: "did:web:hilt.example", wantErr: "hilt_url and hilt_did"},
		{name: "proofs only", proofs: "uZm9v", wantErr: "hilt_url and hilt_did"},
		{
			name: "undecodable proofs", url: "http://127.0.0.1:8080", did: "did:web:hilt.example",
			proofs: "/nonexistent/proofs.cbor", wantErr: "hilt_proofs",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validStandaloneConfig()
			cfg.HiltURL, cfg.HiltDID, cfg.HiltProofs = tc.url, tc.did, tc.proofs
			err := cfg.Validate()
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("expected valid config, got: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("expected error containing %q, got: %v", tc.wantErr, err)
			}
		})
	}
}
