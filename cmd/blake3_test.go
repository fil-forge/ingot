package cmd

import (
	"bytes"
	"encoding/base64"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
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
	require.NoError(t, os.WriteFile(ob, []byte(outboardB64+"\n"), 0o644))

	out, err := run(t, data, "hash")
	require.NoError(t, err, "hash")
	require.Equal(t, want, strings.TrimSpace(out), "hash")
	// --bao prints the attribute's three values, and they drive the ranged
	// commands directly.
	baoOut, err := run(t, data, "hash", "--bao")
	require.NoError(t, err, "hash --bao")
	fields := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(baoOut), "\n") {
		k, v, _ := strings.Cut(line, " ")
		fields[k] = v
	}
	require.Equal(t, want, fields["CID"], "hash --bao printed %q", baoOut)
	require.Equal(t, chunkLog, fields["ChunkLog"], "hash --bao printed %q", baoOut)
	require.Equal(t, outboardB64, fields["Outboard"], "hash --bao printed %q", baoOut)
	out, err = run(t, data[block:], "verify", "--outboard", fields["Outboard"], "--chunk-log", fields["ChunkLog"], "--offset", itoa(block), fields["CID"])
	require.NoError(t, err, "verify from hash --bao output: %q", out)
	require.True(t, strings.HasPrefix(out, "ok bytes "), "verify from hash --bao output: %q", out)
	// --chunk-log chooses the block size: 4 is iroh's, and the outboard is
	// then the Bao library's at chunk log 4; it verifies 16 KiB blocks.
	irohOut, err := run(t, data, "hash", "--bao", "--chunk-log", "4")
	require.NoError(t, err, "hash --bao --chunk-log 4")
	iroh := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(irohOut), "\n") {
		k, v, _ := strings.Cut(line, " ")
		iroh[k] = v
	}
	wantOb, _ := bao.EncodeBuf(data, 4, true)
	require.Equal(t, want, iroh["CID"], "hash --bao --chunk-log 4 printed %q", irohOut)
	require.Equal(t, "4", iroh["ChunkLog"], "hash --bao --chunk-log 4 printed %q", irohOut)
	require.Equal(t, base64.StdEncoding.EncodeToString(wantOb), iroh["Outboard"], "hash --bao --chunk-log 4 printed %q", irohOut)
	out, err = run(t, data[16384:32768], "verify", "--outboard", iroh["Outboard"], "--chunk-log", "4", "--offset", "16384", want)
	require.NoError(t, err, "verify a 16 KiB block against the iroh-sized outboard: %q", out)
	require.True(t, strings.HasPrefix(out, "ok bytes 16384-32767"), "verify a 16 KiB block against the iroh-sized outboard: %q", out)
	_, err = run(t, data, "hash", "--chunk-log", "4")
	require.Error(t, err, "--chunk-log without --bao accepted")
	_, err = run(t, data, "hash", "--bao", "--chunk-log", "60")
	require.Error(t, err, "--chunk-log 60 accepted")

	// An empty object is authenticated by its CID alone: the right one
	// passes with empty stdin, a wrong one is a mismatch, not a pass.
	emptyOut, err := run(t, nil, "hash", "--bao")
	require.NoError(t, err)
	empty := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(emptyOut), "\n") {
		k, v, _ := strings.Cut(line, " ")
		empty[k] = v
	}
	out, err = run(t, nil, "verify", "--outboard", empty["Outboard"], "--chunk-log", empty["ChunkLog"], "--offset", "0", empty["CID"])
	require.NoError(t, err, "empty object with its CID: %q", out)
	require.True(t, strings.HasPrefix(out, "ok empty object"), "empty object with its CID: %q", out)
	var emptyMismatch *mismatchError
	_, err = run(t, nil, "verify", "--outboard", empty["Outboard"], "--chunk-log", empty["ChunkLog"], "--offset", "0", want)
	require.ErrorAs(t, err, &emptyMismatch, "empty object with another object's CID must mismatch")
	out, err = run(t, data, "verify", want)
	require.NoError(t, err, "verify whole: %q", out)
	require.True(t, strings.HasPrefix(out, "ok "), "verify whole: %q", out)
	var mm *mismatchError
	_, err = run(t, data[:size-1], "verify", want)
	require.ErrorAs(t, err, &mm, "verify truncated")
	_, err = run(t, data, "verify", "bafkreigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi")
	require.Error(t, err, "a sha2-256 CID must be a usage error")
	require.NotErrorAs(t, err, &mm, "a sha2-256 CID must be a usage error")

	out, err = run(t, nil, "range", "--outboard-file", ob, "--chunk-log", chunkLog, "5000-200000")
	alignedEnd := min((200000/block+1)*block, size) - 1
	require.NoError(t, err, "range: %q", out)
	require.Contains(t, out, "Range: bytes=0-"+itoa(alignedEnd), "range")
	require.Contains(t, out, "--offset 0", "range")
	_, err = run(t, nil, "range", "5000-200000")
	require.Error(t, err, "range without the attribute flags accepted")

	off := itoa(block)
	// The outboard inline as base64, and from a file, are the same thing.
	for _, ob := range [][]string{{"--outboard", outboardB64}, {"--outboard-file", ob}} {
		args := append(append([]string{"verify"}, ob...), "--chunk-log", chunkLog, "--offset", off, want)
		out, err = run(t, data[block:], args...)
		require.NoError(t, err, "verify range %s: %q", ob[0], out)
		require.True(t, strings.HasPrefix(out, "ok bytes "+off+"-"+itoa(size-1)), "verify range %s: %q", ob[0], out)
	}
	bad := bytes.Clone(data[block:])
	bad[7] ^= 1
	_, err = run(t, bad, "verify", "--outboard", outboardB64, "--chunk-log", chunkLog, "--offset", off, want)
	require.ErrorAs(t, err, &mm, "verify corrupted range")
	_, err = run(t, data[1:], "verify", "--outboard", outboardB64, "--chunk-log", chunkLog, "--offset", "1", want)
	require.Error(t, err, "unaligned offset must be a usage error")
	require.NotErrorAs(t, err, &mm, "unaligned offset must be a usage error")
	_, err = run(t, data[block:], "verify", "--outboard", "not base64!", "--chunk-log", chunkLog, "--offset", off, want)
	require.Error(t, err, "a non-base64 --outboard must be a usage error")
	require.NotErrorAs(t, err, &mm, "a non-base64 --outboard must be a usage error")
	_, err = run(t, data[block:], "verify", "--outboard", outboardB64, "--outboard-file", ob, "--chunk-log", chunkLog, "--offset", off, want)
	require.Error(t, err, "--outboard and --outboard-file together accepted")
	// A chunk log the outboard does not match, or one no block size fits,
	// is a usage error, not a mismatch.
	for _, g := range []string{"7", "53", "63", "255"} {
		_, err = run(t, data[block:], "verify", "--outboard", outboardB64, "--chunk-log", g, "--offset", off, want)
		require.Error(t, err, "--chunk-log %s accepted", g)
		require.NotErrorAs(t, err, &mm, "--chunk-log %s must be a usage error", g)
		_, err = run(t, nil, "range", "--outboard", outboardB64, "--chunk-log", g, "0-1")
		require.Error(t, err, "range --chunk-log %s accepted", g)
	}
}

func itoa(n int64) string { return strconv.FormatInt(n, 10) }
