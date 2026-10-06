package cmd

import (
	"bufio"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
	"github.com/spf13/cobra"
	"lukechampine.com/blake3"

	"github.com/fil-forge/ingot/blake3tree"
)

// newBlake3Cmd groups the client-side checks of the x-cid header and the
// Blake3 object attribute: hash data to its CID, verify data against a CID,
// and work out which bytes to fetch for a verifiable range. Results go to
// stdout so they can be captured (cobra's own Print falls back to stderr);
// errors surface through the root command's exit codes.
func newBlake3Cmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "blake3",
		Short: "Hash and verify object data against ingot's BLAKE3 CIDs",
		Long: `Client-side checks for the x-cid header (a CIDv1 with the raw codec over
the object's BLAKE3 multihash) and the Blake3 attribute of GetObjectAttributes
(the object's CID, Group and Bao Outboard, which verify ranged reads).`,
	}
	c.AddCommand(newBlake3HashCmd(), newBlake3VerifyCmd(), newBlake3RangeCmd())
	return c
}

func newBlake3HashCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "hash",
		Short: "Print the CID of the data on stdin",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := hashCID(cmd.InOrStdin())
			if err != nil {
				return err
			}
			fmt.Fprintln(cmd.OutOrStdout(), c)
			return nil
		},
	}
}

// blake3TreeFlags are the Blake3 attribute's values a ranged check needs:
// the group, and the outboard either inline as the base64 from the XML or
// from a file.
type blake3TreeFlags struct {
	outboard     string
	outboardPath string
	group        uint8
}

func (f *blake3TreeFlags) bind(c *cobra.Command) {
	c.Flags().StringVar(&f.outboard, "outboard", "", "the attribute's Outboard as its base64 string")
	c.Flags().StringVar(&f.outboardPath, "outboard-file", "", "file holding the attribute's Outboard, as the base64 string or as raw bytes")
	c.Flags().Uint8Var(&f.group, "group", 0, "the attribute's Group: block size as a base-2 exponent of bytes")
	c.MarkFlagsMutuallyExclusive("outboard", "outboard-file")
}

// given reports whether any of the flags was set.
func (f *blake3TreeFlags) given() bool {
	return f.outboard != "" || f.outboardPath != "" || f.group != 0
}

// load decodes and checks the outboard. The group and one of the outboard
// flags are required together.
func (f *blake3TreeFlags) load() ([]byte, error) {
	if (f.outboard == "" && f.outboardPath == "") || f.group == 0 {
		return nil, errors.New("--group and one of --outboard or --outboard-file are required together")
	}
	raw, src := []byte(f.outboard), "--outboard"
	if f.outboardPath != "" {
		var err error
		if raw, err = os.ReadFile(f.outboardPath); err != nil {
			return nil, err
		}
		src = f.outboardPath
	}
	outboard := raw
	if dec, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(raw))); err == nil {
		outboard = dec
	} else if f.outboardPath == "" {
		return nil, fmt.Errorf("--outboard is not base64: %w", err)
	}
	// The group and the outboard must agree with each other (and the group
	// must be one a block size can be computed from) before either is used.
	if _, err := blake3tree.OutboardLeaves(outboard, f.group); err != nil {
		return nil, fmt.Errorf("%s with --group %d: %w", src, f.group, err)
	}
	return outboard, nil
}

func newBlake3VerifyCmd() *cobra.Command {
	var tree blake3TreeFlags
	var offset int64
	c := &cobra.Command{
		Use:   "verify <cid>",
		Short: "Verify the data on stdin against an object CID",
		Long: `Verify the data on stdin against an object CID, the x-cid header's value.

Without flags the whole object is expected and hashed. With --group,
--offset and the outboard (--outboard as the attribute's base64 string, or
--outboard-file) the data is a block-aligned range of the object (see
"ingot blake3 range"), checked block by block against the Bao outboard.

Exit status 1 means the data does not match; any other failure is 2.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			want, err := parseObjectCID(args[0])
			if err != nil {
				return err
			}
			in := bufio.NewReaderSize(cmd.InOrStdin(), 1<<20)
			if !tree.given() && !cmd.Flags().Changed("offset") {
				got, err := hashCID(in)
				if err != nil {
					return err
				}
				if !got.Equals(want) {
					return &mismatchError{fmt.Sprintf("mismatch\n  want %s\n  got  %s", want, got)}
				}
				fmt.Fprintln(cmd.OutOrStdout(), "ok", got)
				return nil
			}
			outboard, err := tree.load()
			if err != nil {
				return err
			}
			var root blake3tree.CV
			dec, _ := mh.Decode(want.Hash())
			copy(root[:], dec.Digest)
			n, err := blake3tree.VerifyBlocks(in, outboard, tree.group, offset, root)
			var bad *blake3tree.BlockError
			if errors.As(err, &bad) {
				return &mismatchError{err.Error()}
			}
			if err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "ok bytes %d-%d of %s\n", offset, offset+n-1, want)
			return nil
		},
	}
	tree.bind(c)
	c.Flags().Int64Var(&offset, "offset", 0, "object byte offset of the first byte on stdin (ranged mode)")
	return c
}

func newBlake3RangeCmd() *cobra.Command {
	var tree blake3TreeFlags
	c := &cobra.Command{
		Use:   "range <start-end>",
		Short: "Print the block-aligned byte range to fetch to verify the given bytes",
		Long: `Print the block-aligned byte range that covers the inclusive byte range
start-end, as the Range header to request and the --offset to verify it
with. A block is the only unit the outboard can verify, so a range widens to
block boundaries, or to the object's end.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			outboard, err := tree.load()
			if err != nil {
				return err
			}
			a, b, err := parseByteRange(args[0])
			if err != nil {
				return err
			}
			start, end, err := blake3tree.AlignedRange(a, b, tree.group, blake3tree.OutboardSize(outboard))
			if err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Range: bytes=%d-%d\n--offset %d\n", start, end, start)
			return nil
		},
	}
	tree.bind(c)
	return c
}

// mismatchError is a verification failure, as opposed to a bad input; the
// root command exits 1 for it.
type mismatchError struct{ msg string }

func (e *mismatchError) Error() string { return e.msg }

// hashCID returns the raw-codec BLAKE3 CID of r.
func hashCID(r io.Reader) (cid.Cid, error) {
	h := blake3.New(32, nil)
	if _, err := io.Copy(h, r); err != nil {
		return cid.Undef, fmt.Errorf("read stdin: %w", err)
	}
	digest, err := mh.Encode(h.Sum(nil), mh.BLAKE3)
	if err != nil {
		return cid.Undef, err
	}
	return cid.NewCidV1(cid.Raw, digest), nil
}

// parseObjectCID accepts a raw-codec CID over a blake3 multihash and nothing
// else, so a sha2-256 CID cannot pass as a check.
func parseObjectCID(s string) (cid.Cid, error) {
	c, err := cid.Decode(s)
	if err != nil {
		return cid.Undef, fmt.Errorf("not a CID: %w", err)
	}
	if c.Type() != cid.Raw {
		return cid.Undef, fmt.Errorf("CID codec is %#x, want raw (%#x)", c.Type(), cid.Raw)
	}
	if dec, err := mh.Decode(c.Hash()); err != nil || dec.Code != mh.BLAKE3 || len(dec.Digest) != blake3tree.CVSize {
		return cid.Undef, errors.New("CID multihash is not a 32-byte blake3 digest")
	}
	return c, nil
}

// parseByteRange reads an inclusive "start-end" range, with or without the
// Range header's "bytes=" prefix.
func parseByteRange(s string) (a, b int64, err error) {
	lo, hi, ok := strings.Cut(strings.TrimPrefix(s, "bytes="), "-")
	if !ok {
		return 0, 0, fmt.Errorf("range %q is not start-end", s)
	}
	if a, err = strconv.ParseInt(lo, 10, 64); err != nil || a < 0 {
		return 0, 0, fmt.Errorf("range start %q", lo)
	}
	if b, err = strconv.ParseInt(hi, 10, 64); err != nil || b < a {
		return 0, 0, fmt.Errorf("range end %q", hi)
	}
	return a, b, nil
}
