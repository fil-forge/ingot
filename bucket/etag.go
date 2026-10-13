package bucket

import (
	"hash"
	"sync"

	md5simd "github.com/minio/md5-simd"
)

// etagServer is the process-wide md5-simd server behind every ETag hash.
// On amd64 with AVX2 or AVX-512 it packs up to 8 or 16 concurrent MD5
// streams into one core's vector unit, so a node ingesting many parts at
// once spends several times fewer cores on MD5; a single stream is no faster
// than crypto/md5. On every other architecture the server is a thin wrapper
// over crypto/md5. The server lives for the process: its lanes are shared by
// all in-flight bodies and it is never closed.
var (
	etagServerOnce sync.Once
	etagServer     md5simd.Server
)

// newETagHash returns a fresh MD5 hasher on the shared server. The returned
// hash must be closed once summed (asyncHash does this) so its lane returns
// to the server.
func newETagHash() hash.Hash {
	etagServerOnce.Do(func() { etagServer = md5simd.NewServer() })
	return etagServer.NewHash()
}
