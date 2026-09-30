package s3frontend

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdh"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/fil-forge/ucantone/did"
	"github.com/filecoin-project/go-fee"
	"github.com/filecoin-project/go-fee/aesstream"
	mh "github.com/multiformats/go-multihash"

	"github.com/fil-forge/ingot/blockstore"
	msbucket "github.com/fil-forge/ingot/bucket"
	"github.com/fil-forge/ingot/regionkey"
	"github.com/fil-forge/ingot/registry"
)

// These benchmarks measure go-fee as the body write and read paths drive it:
// SplitBody → encryptingBlobWriter → fee.EncryptWithCEK → the spool's
// sha256+file copy on the way in, and decryptingOpener → aesstream.SpanReader
// over a spool section on the way out. The layered variants peel the pipeline
// apart so each pass can be attributed: the disk, the spool hash, the body
// hashes, the FEE pipe, and the AES-GCM floor.

var benchSizes = []int64{4 << 10, 1 << 20, 16 << 20, 64 << 20, msbucket.DefaultMaxBlobSize}

func sizeName(n int64) string {
	switch {
	case n == msbucket.DefaultMaxBlobSize:
		return "maxblob"
	case n >= 1<<20:
		return fmt.Sprintf("%dMiB", n>>20)
	default:
		return fmt.Sprintf("%dKiB", n>>10)
	}
}

func benchData(n int64) []byte {
	data := make([]byte, n)
	if _, err := rand.Read(data); err != nil {
		panic(err)
	}
	return data
}

type benchEnc struct {
	spool     *blockstore.Spool
	keys      regionkey.Provider
	space     did.DID
	recipient fee.Recipient
}

func newBenchEnc(tb testing.TB) *benchEnc {
	tb.Helper()
	spool, err := blockstore.NewSpool(filepath.Join(tb.TempDir(), "spool"))
	if err != nil {
		tb.Fatal(err)
	}
	kek := make([]byte, regionkey.KEKLen)
	if _, err := rand.Read(kek); err != nil {
		tb.Fatal(err)
	}
	keys, err := regionkey.NewInProcessProvider("v1", kek)
	if err != nil {
		tb.Fatal(err)
	}
	priv, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		tb.Fatal(err)
	}
	space, err := did.Parse("did:web:bench.example")
	if err != nil {
		tb.Fatal(err)
	}
	return &benchEnc{
		spool:     spool,
		keys:      keys,
		space:     space,
		recipient: fee.NewECDHESRecipient([]byte("bench-kid"), priv.PublicKey()),
	}
}

// hashingDiscardWriter is Spool.WriteBlob without the file: the sha256 pass
// over the envelope and nothing else.
type hashingDiscardWriter struct{}

func (hashingDiscardWriter) WriteBlob(_ context.Context, r io.Reader) (mh.Multihash, int64, error) {
	h := sha256.New()
	n, err := io.Copy(h, r)
	if err != nil || n == 0 {
		return nil, n, err
	}
	digest, err := mh.Encode(h.Sum(nil), mh.SHA2_256)
	return digest, n, err
}

// discardWriter drains the envelope and names it by a counter, so the
// encrypting writer's map stays happy without any hash pass.
type discardWriter struct{ n int }

func (d *discardWriter) WriteBlob(_ context.Context, r io.Reader) (mh.Multihash, int64, error) {
	n, err := io.Copy(io.Discard, r)
	if err != nil || n == 0 {
		return nil, n, err
	}
	d.n++
	sum := sha256.Sum256(fmt.Append(nil, d.n))
	digest, err := mh.Encode(sum[:], mh.SHA2_256)
	return digest, n, err
}

// BenchmarkIngest_Full is the production write path end to end: SplitBody's
// body sha256 + async md5, EncryptWithCEK with the tenant recipient, the
// spool's sha256 + temp file + rename, and the region-key wrap.
func BenchmarkIngest_Full(b *testing.B) {
	for _, size := range benchSizes {
		b.Run(sizeName(size), func(b *testing.B) {
			env := newBenchEnc(b)
			data := benchData(size)
			b.SetBytes(size)
			b.ReportAllocs()
			for b.Loop() {
				enc := newEncryptingBlobWriter(env.spool, env.keys, env.space, []fee.Recipient{env.recipient})
				body, err := msbucket.SplitBody(context.Background(), enc, bytes.NewReader(data), 0)
				if err != nil {
					b.Fatal(err)
				}
				for _, ref := range body.Blobs {
					_ = env.spool.Remove(ref.Digest)
				}
			}
		})
	}
}

// BenchmarkIngest_NoDisk is the same path with the spool's file replaced by
// nothing: the three hash passes, the FEE pipe and the wrap remain.
func BenchmarkIngest_NoDisk(b *testing.B) {
	for _, size := range benchSizes {
		b.Run(sizeName(size), func(b *testing.B) {
			env := newBenchEnc(b)
			data := benchData(size)
			b.SetBytes(size)
			b.ReportAllocs()
			for b.Loop() {
				enc := newEncryptingBlobWriter(hashingDiscardWriter{}, env.keys, env.space, []fee.Recipient{env.recipient})
				if _, err := msbucket.SplitBody(context.Background(), enc, bytes.NewReader(data), 0); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkIngest_NoSpoolHash drops the spool's sha256 too: SplitBody's body
// hashes and the FEE pipe only.
func BenchmarkIngest_NoSpoolHash(b *testing.B) {
	for _, size := range benchSizes {
		b.Run(sizeName(size), func(b *testing.B) {
			env := newBenchEnc(b)
			data := benchData(size)
			b.SetBytes(size)
			b.ReportAllocs()
			for b.Loop() {
				enc := newEncryptingBlobWriter(&discardWriter{}, env.keys, env.space, []fee.Recipient{env.recipient})
				if _, err := msbucket.SplitBody(context.Background(), enc, bytes.NewReader(data), 0); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkFEE_EncryptWithCEK is go-fee alone as ingot calls it: one
// ECDH-ES recipient, the default chunk size, drained by io.Copy into
// io.Discard (the pipe, its goroutine and AES-GCM; no hashing).
func BenchmarkFEE_EncryptWithCEK(b *testing.B) {
	for _, size := range benchSizes {
		b.Run(sizeName(size), func(b *testing.B) {
			env := newBenchEnc(b)
			data := benchData(size)
			cek := make([]byte, 32)
			rand.Read(cek)
			b.SetBytes(size)
			b.ReportAllocs()
			for b.Loop() {
				rc, _, err := fee.EncryptWithCEK(bytes.NewReader(data), cek, []fee.Recipient{env.recipient})
				if err != nil {
					b.Fatal(err)
				}
				if _, err := io.Copy(io.Discard, rc); err != nil {
					b.Fatal(err)
				}
				rc.Close()
			}
		})
	}
}

// BenchmarkFEE_EncryptWithCEK_NoRecipient isolates the per-call ECDH wrap:
// the same stream as a recipient-less COSE_Encrypt0.
func BenchmarkFEE_EncryptWithCEK_NoRecipient(b *testing.B) {
	for _, size := range benchSizes[:2] {
		b.Run(sizeName(size), func(b *testing.B) {
			data := benchData(size)
			cek := make([]byte, 32)
			rand.Read(cek)
			b.SetBytes(size)
			b.ReportAllocs()
			for b.Loop() {
				rc, _, err := fee.EncryptWithCEK(bytes.NewReader(data), cek, nil)
				if err != nil {
					b.Fatal(err)
				}
				if _, err := io.Copy(io.Discard, rc); err != nil {
					b.Fatal(err)
				}
				rc.Close()
			}
		})
	}
}

// BenchmarkAESStream_Writer is the body cipher without the pipe: io.Copy from
// the plaintext straight into aesstream.Writer over io.Discard.
func BenchmarkAESStream_Writer(b *testing.B) {
	for _, size := range benchSizes {
		b.Run(sizeName(size), func(b *testing.B) {
			data := benchData(size)
			cfg := aesstream.Config{Key: make([]byte, 32), BaseNonce: make([]byte, 7), AAD: []byte("aad")}
			b.SetBytes(size)
			b.ReportAllocs()
			for b.Loop() {
				w, err := aesstream.NewWriter(io.Discard, cfg)
				if err != nil {
					b.Fatal(err)
				}
				if _, err := io.Copy(w, bytes.NewReader(data)); err != nil {
					b.Fatal(err)
				}
				if err := w.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkGCMSeal is the hardware floor: AES-256-GCM Seal over 256 KiB
// chunks with no framing, buffering or I/O.
func BenchmarkGCMSeal(b *testing.B) {
	const size = 64 << 20
	data := benchData(size)
	block, _ := aes.NewCipher(make([]byte, 32))
	aead, _ := cipher.NewGCM(block)
	nonce := make([]byte, 12)
	out := make([]byte, 0, aesstream.DefaultChunkSize+aesstream.TagSize)
	b.SetBytes(size)
	for b.Loop() {
		for off := 0; off < size; off += aesstream.DefaultChunkSize {
			out = aead.Seal(out[:0], nonce, data[off:off+aesstream.DefaultChunkSize], nil)
		}
	}
}

// BenchmarkSHA256 is one sha256 pass over the data (ingot pays two per body:
// the plaintext body hash and the spool's ciphertext hash).
func BenchmarkSHA256(b *testing.B) {
	const size = 64 << 20
	data := benchData(size)
	b.SetBytes(size)
	for b.Loop() {
		h := sha256.New()
		h.Write(data)
		h.Sum(nil)
	}
}

// BenchmarkFileWrite is the spool's disk pass alone: io.Copy of the data into
// a temp file in 32 KiB pieces, then rename, as Spool.WriteBlob does.
func BenchmarkFileWrite(b *testing.B) {
	for _, size := range benchSizes[1:] {
		b.Run(sizeName(size), func(b *testing.B) {
			dir := b.TempDir()
			data := benchData(size)
			b.SetBytes(size)
			for b.Loop() {
				f, err := os.CreateTemp(dir, ".tmp-*")
				if err != nil {
					b.Fatal(err)
				}
				if _, err := io.Copy(f, bytes.NewReader(data)); err != nil {
					b.Fatal(err)
				}
				f.Close()
				dst := filepath.Join(dir, "blob")
				if err := os.Rename(f.Name(), dst); err != nil {
					b.Fatal(err)
				}
				os.Remove(dst)
			}
		})
	}
}

// benchStored writes one body through the production path and returns the
// state the read path needs: the plaintext, the body and the decrypting
// opener with its prefetched params, exactly as bodyOpener would build it.
func benchStored(b *testing.B, env *benchEnc, data []byte) (msbucket.Body, *decryptingOpener) {
	b.Helper()
	enc := newEncryptingBlobWriter(env.spool, env.keys, env.space, []fee.Recipient{env.recipient})
	body, err := msbucket.SplitBody(context.Background(), enc, bytes.NewReader(data), 0)
	if err != nil {
		b.Fatal(err)
	}
	encMap := make(map[string]encBlob)
	for _, ref := range body.Blobs {
		params, err := enc.params(env.space, ref.Digest)
		if err != nil {
			b.Fatal(err)
		}
		stored, err := enc.storedSize(ref.Digest)
		if err != nil {
			b.Fatal(err)
		}
		encMap[string(ref.Digest)] = encBlob{params: params, storedSize: stored}
	}
	return body, &decryptingOpener{read: env.spool, keys: env.keys, enc: encMap}
}

// BenchmarkRead_Full is the production read path: OpenBody over the
// decrypting opener (region-key unwrap, spool section, SpanReader), drained
// with io.Copy's 32 KiB reads as the response writer does.
func BenchmarkRead_Full(b *testing.B) {
	for _, size := range benchSizes {
		b.Run(sizeName(size), func(b *testing.B) {
			env := newBenchEnc(b)
			data := benchData(size)
			body, opener := benchStored(b, env, data)
			b.SetBytes(size)
			b.ReportAllocs()
			for b.Loop() {
				rc := msbucket.OpenBody(context.Background(), opener, env.space, body)
				n, err := io.Copy(io.Discard, rc)
				rc.Close()
				if err != nil {
					b.Fatal(err)
				}
				if n != size {
					b.Fatalf("read %d bytes, want %d", n, size)
				}
			}
		})
	}
}

// BenchmarkRead_Full4K drains the same path in 4 KiB reads, fasthttp's
// copy-buffer size when the response cannot be handed to the connection.
func BenchmarkRead_Full4K(b *testing.B) {
	for _, size := range benchSizes[1:3] {
		b.Run(sizeName(size), func(b *testing.B) {
			env := newBenchEnc(b)
			data := benchData(size)
			body, opener := benchStored(b, env, data)
			buf := make([]byte, 4096)
			b.SetBytes(size)
			b.ReportAllocs()
			for b.Loop() {
				rc := msbucket.OpenBody(context.Background(), opener, env.space, body)
				_, err := io.CopyBuffer(io.Discard, struct{ io.Reader }{rc}, buf)
				rc.Close()
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkSpanReader_Memory is aesstream.SpanReader alone over an in-memory
// ciphertext: AES-GCM Open plus the reader's copies, no disk.
func BenchmarkSpanReader_Memory(b *testing.B) {
	for _, size := range benchSizes {
		b.Run(sizeName(size), func(b *testing.B) {
			data := benchData(size)
			cfg := aesstream.Config{Key: make([]byte, 32), BaseNonce: make([]byte, 7), AAD: []byte("aad")}
			ct, err := aesstream.Seal(cfg, data)
			if err != nil {
				b.Fatal(err)
			}
			b.SetBytes(size)
			b.ReportAllocs()
			for b.Loop() {
				sr, err := aesstream.NewSpanReader(bytes.NewReader(ct), cfg, int64(len(ct)), 0, size-1)
				if err != nil {
					b.Fatal(err)
				}
				if _, err := io.Copy(io.Discard, sr); err != nil {
					b.Fatal(err)
				}
				sr.Close()
			}
		})
	}
}

// BenchmarkRead_Range is a 1 MiB ranged GET from the middle of the largest
// blob: the fixed per-request cost (unwrap, open, first-chunk trim) over a
// short span.
func BenchmarkRead_Range(b *testing.B) {
	env := newBenchEnc(b)
	size := msbucket.DefaultMaxBlobSize
	data := benchData(size)
	body, opener := benchStored(b, env, data)
	const rangeLen = 1 << 20
	start := size/2 + 12345
	b.SetBytes(rangeLen)
	b.ReportAllocs()
	for b.Loop() {
		rc := msbucket.OpenBodyRange(context.Background(), opener, env.space, body, start, start+rangeLen-1)
		n, err := io.Copy(io.Discard, rc)
		rc.Close()
		if err != nil {
			b.Fatal(err)
		}
		if n != rangeLen {
			b.Fatalf("read %d bytes, want %d", n, rangeLen)
		}
	}
}

var _ = registry.BlobEncryptionParams{}

// BenchmarkIngest_NoDiskParallel is the aggregate: GOMAXPROCS bodies in
// flight through the no-disk path, which is what a loaded gateway pays.
func BenchmarkIngest_NoDiskParallel(b *testing.B) {
	const size = 16 << 20
	env := newBenchEnc(b)
	data := benchData(size)
	b.SetBytes(size)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			enc := newEncryptingBlobWriter(hashingDiscardWriter{}, env.keys, env.space, []fee.Recipient{env.recipient})
			if _, err := msbucket.SplitBody(context.Background(), enc, bytes.NewReader(data), 0); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkFEE_EncryptWithCEKParallel is go-fee alone with GOMAXPROCS
// streams in flight.
func BenchmarkFEE_EncryptWithCEKParallel(b *testing.B) {
	const size = 16 << 20
	env := newBenchEnc(b)
	data := benchData(size)
	cek := make([]byte, 32)
	rand.Read(cek)
	b.SetBytes(size)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			rc, _, err := fee.EncryptWithCEK(bytes.NewReader(data), cek, []fee.Recipient{env.recipient})
			if err != nil {
				b.Fatal(err)
			}
			if _, err := io.Copy(io.Discard, rc); err != nil {
				b.Fatal(err)
			}
			rc.Close()
		}
	})
}

// BenchmarkMD5 is one crypto/md5 pass, the ETag hash SplitBody runs aside.
func BenchmarkMD5(b *testing.B) {
	const size = 64 << 20
	data := benchData(size)
	b.SetBytes(size)
	for b.Loop() {
		h := md5.New()
		h.Write(data)
		h.Sum(nil)
	}
}
