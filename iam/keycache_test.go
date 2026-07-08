package iam_test

import (
	"testing"
	"time"

	s3 "github.com/fil-forge/libforge/commands/s3"
	"github.com/stretchr/testify/require"

	"github.com/fil-forge/ingot/iam"
)

func TestVerificationKeyCache(t *testing.T) {
	c := iam.NewVerificationKeyCache()
	c.Put("access-1", time.Hour,
		s3.VerificationKey{Kind: s3.KeyKindSigV4, Data: []byte("hmac-key")},
		s3.VerificationKey{Kind: s3.KeyKindSigV4a, Data: []byte("ecdsa-key")},
	)

	t.Run("get by kind", func(t *testing.T) {
		got, ok := c.Get("access-1", s3.KeyKindSigV4)
		require.True(t, ok)
		require.Equal(t, []byte("hmac-key"), got)

		got, ok = c.Get("access-1", s3.KeyKindSigV4a)
		require.True(t, ok)
		require.Equal(t, []byte("ecdsa-key"), got)
	})

	t.Run("miss on unknown access or kind", func(t *testing.T) {
		_, ok := c.Get("access-2", s3.KeyKindSigV4)
		require.False(t, ok)
		_, ok = c.Get("access-1", "sigv9")
		require.False(t, ok)
	})

	t.Run("empty-data keys are skipped", func(t *testing.T) {
		c.Put("access-3", time.Hour, s3.VerificationKey{Kind: s3.KeyKindSigV4, Data: nil})
		_, ok := c.Get("access-3", s3.KeyKindSigV4)
		require.False(t, ok)
	})

	t.Run("expired entries are gone", func(t *testing.T) {
		c.Put("access-4", 20*time.Millisecond, s3.VerificationKey{Kind: s3.KeyKindSigV4, Data: []byte("x")})
		time.Sleep(40 * time.Millisecond)
		_, ok := c.Get("access-4", s3.KeyKindSigV4)
		require.False(t, ok)
	})

	t.Run("non-positive ttl caches nothing", func(t *testing.T) {
		c.Put("access-5", 0, s3.VerificationKey{Kind: s3.KeyKindSigV4, Data: []byte("x")})
		_, ok := c.Get("access-5", s3.KeyKindSigV4)
		require.False(t, ok)
	})
}
