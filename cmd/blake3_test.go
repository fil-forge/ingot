package cmd

import (
	"bytes"
	"encoding/base64"
	"errors"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
	"lukechampine.com/blake3/bao"

	"github.com/fil-forge/ingot/blake3tree"
)

// run executes `ingot blake3 args...` with stdin and returns its output.
func run(t *testing.T, stdin []byte, args ...string) (string, error) {
	t.Helper()
	root := newRootCmd()
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&out)
	root.SetIn(bytes.NewReader(stdin))
	root.SetArgs(append([]string{"blake3"}, args...))
	err := root.Execute()
	return out.String(), err
}

func TestBlake3Commands(t *testing.T) {
	const size = 300_000
	data := make([]byte, size)
	rand.New(rand.NewSource(1)).Read(data)
	h, _ := blake3tree.NewHasher(0)
	h.Write(data)
	obj := h.FinishObject()
	digest, _ := mh.Encode(obj.Root[:], mh.BLAKE3)
	want := cid.NewCidV1(cid.Raw, digest).String()
	block := blake3tree.BlockSize(obj.ChunkLog)
	chunkLog := itoa(int64(obj.ChunkLog))
	outboardB64 := base64.StdEncoding.EncodeToString(blake3tree.Outboard(obj.Blocks, size))
	ob := filepath.Join(t.TempDir(), "ob.b64")
	if err := os.WriteFile(ob, []byte(outboardB64+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	if out, err := run(t, data, "hash"); err != nil || strings.TrimSpace(out) != want {
		t.Fatalf("hash: %q, %v", out, err)
	}
	// --bao prints the attribute's three values, and they drive the ranged
	// commands directly.
	baoOut, err := run(t, data, "hash", "--bao")
	if err != nil {
		t.Fatalf("hash --bao: %v", err)
	}
	fields := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(baoOut), "\n") {
		k, v, _ := strings.Cut(line, " ")
		fields[k] = v
	}
	if fields["CID"] != want || fields["ChunkLog"] != chunkLog || fields["Outboard"] != outboardB64 {
		t.Fatalf("hash --bao printed %q", baoOut)
	}
	if out, err := run(t, data[block:], "verify", "--outboard", fields["Outboard"], "--chunk-log", fields["ChunkLog"], "--offset", itoa(block), fields["CID"]); err != nil || !strings.HasPrefix(out, "ok bytes ") {
		t.Fatalf("verify from hash --bao output: %q, %v", out, err)
	}
	// --chunk-log chooses the block size: 4 is iroh's, and the outboard is
	// then the Bao library's at chunk log 4; it verifies 16 KiB blocks.
	irohOut, err := run(t, data, "hash", "--bao", "--chunk-log", "4")
	if err != nil {
		t.Fatalf("hash --bao --chunk-log 4: %v", err)
	}
	iroh := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(irohOut), "\n") {
		k, v, _ := strings.Cut(line, " ")
		iroh[k] = v
	}
	wantOb, _ := bao.EncodeBuf(data, 4, true)
	if iroh["CID"] != want || iroh["ChunkLog"] != "4" || iroh["Outboard"] != base64.StdEncoding.EncodeToString(wantOb) {
		t.Fatalf("hash --bao --chunk-log 4 printed %q", irohOut)
	}
	if out, err := run(t, data[16384:32768], "verify", "--outboard", iroh["Outboard"], "--chunk-log", "4", "--offset", "16384", want); err != nil || !strings.HasPrefix(out, "ok bytes 16384-32767") {
		t.Fatalf("verify a 16 KiB block against the iroh-sized outboard: %q, %v", out, err)
	}
	if _, err := run(t, data, "hash", "--chunk-log", "4"); err == nil {
		t.Fatal("--chunk-log without --bao accepted")
	}
	if _, err := run(t, data, "hash", "--bao", "--chunk-log", "60"); err == nil {
		t.Fatal("--chunk-log 60 accepted")
	}

	// An empty object is authenticated by its CID alone: the right one
	// passes with empty stdin, a wrong one is a mismatch, not a pass.
	emptyOut, err := run(t, nil, "hash", "--bao")
	if err != nil {
		t.Fatal(err)
	}
	empty := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(emptyOut), "\n") {
		k, v, _ := strings.Cut(line, " ")
		empty[k] = v
	}
	if out, err := run(t, nil, "verify", "--outboard", empty["Outboard"], "--chunk-log", empty["ChunkLog"], "--offset", "0", empty["CID"]); err != nil || !strings.HasPrefix(out, "ok empty object") {
		t.Fatalf("empty object with its CID: %q, %v", out, err)
	}
	var emptyMismatch *mismatchError
	if _, err := run(t, nil, "verify", "--outboard", empty["Outboard"], "--chunk-log", empty["ChunkLog"], "--offset", "0", want); !errors.As(err, &emptyMismatch) {
		t.Fatalf("empty object with another object's CID must mismatch: %v", err)
	}
	if out, err := run(t, data, "verify", want); err != nil || !strings.HasPrefix(out, "ok ") {
		t.Fatalf("verify whole: %q, %v", out, err)
	}
	var mm *mismatchError
	if _, err := run(t, data[:size-1], "verify", want); !errors.As(err, &mm) {
		t.Fatalf("verify truncated: %v", err)
	}
	if _, err := run(t, data, "verify", "bafkreigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi"); err == nil || errors.As(err, &mm) {
		t.Fatalf("a sha2-256 CID must be a usage error: %v", err)
	}

	out, err := run(t, nil, "range", "--outboard-file", ob, "--chunk-log", chunkLog, "5000-200000")
	if err != nil || !strings.Contains(out, "Range: bytes=0-"+itoa(2*block-1)) || !strings.Contains(out, "--offset 0") {
		t.Fatalf("range: %q, %v", out, err)
	}
	if _, err := run(t, nil, "range", "5000-200000"); err == nil {
		t.Fatal("range without the attribute flags accepted")
	}

	off := itoa(block)
	// The outboard inline as base64, and from a file, are the same thing.
	for _, ob := range [][]string{{"--outboard", outboardB64}, {"--outboard-file", ob}} {
		args := append(append([]string{"verify"}, ob...), "--chunk-log", chunkLog, "--offset", off, want)
		if out, err := run(t, data[block:], args...); err != nil || !strings.HasPrefix(out, "ok bytes "+off+"-"+itoa(size-1)) {
			t.Fatalf("verify range %s: %q, %v", ob[0], out, err)
		}
	}
	bad := bytes.Clone(data[block:])
	bad[7] ^= 1
	if _, err := run(t, bad, "verify", "--outboard", outboardB64, "--chunk-log", chunkLog, "--offset", off, want); !errors.As(err, &mm) {
		t.Fatalf("verify corrupted range: %v", err)
	}
	if _, err := run(t, data[1:], "verify", "--outboard", outboardB64, "--chunk-log", chunkLog, "--offset", "1", want); err == nil || errors.As(err, &mm) {
		t.Fatalf("unaligned offset must be a usage error: %v", err)
	}
	if _, err := run(t, data[block:], "verify", "--outboard", "not base64!", "--chunk-log", chunkLog, "--offset", off, want); err == nil || errors.As(err, &mm) {
		t.Fatalf("a non-base64 --outboard must be a usage error: %v", err)
	}
	if _, err := run(t, data[block:], "verify", "--outboard", outboardB64, "--outboard-file", ob, "--chunk-log", chunkLog, "--offset", off, want); err == nil {
		t.Fatal("--outboard and --outboard-file together accepted")
	}
	// A chunk log the outboard does not match, or one no block size fits,
	// is a usage error, not a mismatch.
	for _, g := range []string{"4", "53", "63", "255"} {
		if _, err := run(t, data[block:], "verify", "--outboard", outboardB64, "--chunk-log", g, "--offset", off, want); err == nil || errors.As(err, &mm) {
			t.Fatalf("--chunk-log %s accepted: %v", g, err)
		}
		if _, err := run(t, nil, "range", "--outboard", outboardB64, "--chunk-log", g, "0-1"); err == nil {
			t.Fatalf("range --chunk-log %s accepted", g)
		}
	}
}

func itoa(n int64) string { return strconv.FormatInt(n, 10) }
