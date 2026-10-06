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
	block := blake3tree.GroupSize(obj.GroupLog)
	group := itoa(int64(obj.GroupLog))
	outboardB64 := base64.StdEncoding.EncodeToString(blake3tree.Outboard(obj.Leaves, size))
	ob := filepath.Join(t.TempDir(), "ob.b64")
	if err := os.WriteFile(ob, []byte(outboardB64+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	if out, err := run(t, data, "hash"); err != nil || strings.TrimSpace(out) != want {
		t.Fatalf("hash: %q, %v", out, err)
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

	out, err := run(t, nil, "range", "--outboard-file", ob, "--group", group, "5000-200000")
	if err != nil || !strings.Contains(out, "Range: bytes=0-"+itoa(2*block-1)) || !strings.Contains(out, "--offset 0") {
		t.Fatalf("range: %q, %v", out, err)
	}
	if _, err := run(t, nil, "range", "5000-200000"); err == nil {
		t.Fatal("range without the attribute flags accepted")
	}

	off := itoa(block)
	// The outboard inline as base64, and from a file, are the same thing.
	for _, ob := range [][]string{{"--outboard", outboardB64}, {"--outboard-file", ob}} {
		args := append(append([]string{"verify"}, ob...), "--group", group, "--offset", off, want)
		if out, err := run(t, data[block:], args...); err != nil || !strings.HasPrefix(out, "ok bytes "+off+"-"+itoa(size-1)) {
			t.Fatalf("verify range %s: %q, %v", ob[0], out, err)
		}
	}
	bad := bytes.Clone(data[block:])
	bad[7] ^= 1
	if _, err := run(t, bad, "verify", "--outboard", outboardB64, "--group", group, "--offset", off, want); !errors.As(err, &mm) {
		t.Fatalf("verify corrupted range: %v", err)
	}
	if _, err := run(t, data[1:], "verify", "--outboard", outboardB64, "--group", group, "--offset", "1", want); err == nil || errors.As(err, &mm) {
		t.Fatalf("unaligned offset must be a usage error: %v", err)
	}
	if _, err := run(t, data[block:], "verify", "--outboard", "not base64!", "--group", group, "--offset", off, want); err == nil || errors.As(err, &mm) {
		t.Fatalf("a non-base64 --outboard must be a usage error: %v", err)
	}
	if _, err := run(t, data[block:], "verify", "--outboard", outboardB64, "--outboard-file", ob, "--group", group, "--offset", off, want); err == nil {
		t.Fatal("--outboard and --outboard-file together accepted")
	}
	// A group the outboard does not match, or one no block size fits, is a
	// usage error, not a mismatch.
	for _, g := range []string{"14", "63", "64", "255"} {
		if _, err := run(t, data[block:], "verify", "--outboard", outboardB64, "--group", g, "--offset", off, want); err == nil || errors.As(err, &mm) {
			t.Fatalf("--group %s accepted: %v", g, err)
		}
		if _, err := run(t, nil, "range", "--outboard", outboardB64, "--group", g, "0-1"); err == nil {
			t.Fatalf("range --group %s accepted", g)
		}
	}
}

func itoa(n int64) string { return strconv.FormatInt(n, 10) }
