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
(the object's CID, ChunkLog and Bao Outboard, which verify ranged reads).`,
	}
	c.AddCommand(newBlake3HashCmd(), newBlake3VerifyCmd(), newBlake3RangeCmd())
	return c
}

func newBlake3HashCmd() *cobra.Command {
	var bao bool
	var chunkLog uint8
	c := &cobra.Command{
		Use:   "hash",
		Short: "Print the CID of the data on stdin",
		Long: `Print the CID of the data on stdin: the x-cid header ingot returns for it.

With --bao, print what the Blake3 attribute of GetObjectAttributes would
carry for it, one per line: CID, ChunkLog (the Bao block size as a base-2
exponent of 1 KiB BLAKE3 chunks) and Outboard (the Bao outboard, base64),
the inputs of "ingot blake3 range" and the ranged "ingot blake3 verify".
The chunk log is the one ingot would record for a body of this size unless
--chunk-log chooses another. For an outboard iroh can use, set --chunk-log
4, iroh's fixed 16 KiB block; 0 is the original Bao format. The blocks are
held in memory while hashing, 32 bytes per block.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			in := bufio.NewReaderSize(cmd.InOrStdin(), 1<<20)
			if !bao {
				if cmd.Flags().Changed("chunk-log") {
					return errors.New("--chunk-log needs --bao")
				}
				c, err := hashCID(in)
				if err != nil {
					return err
				}
				fmt.Fprintln(cmd.OutOrStdout(), c)
				return nil
			}
			h, err := blake3tree.NewHasher(0)
			if cmd.Flags().Changed("chunk-log") {
				h, err = blake3tree.NewHasherAtChunkLog(0, chunkLog)
			}
			if err != nil {
				return err
			}
			if _, err := io.Copy(h, in); err != nil {
				return fmt.Errorf("read stdin: %w", err)
			}
			obj := h.FinishObject()
			digest, err := mh.Encode(obj.Root[:], mh.BLAKE3)
			if err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "CID %s\nChunkLog %d\nOutboard %s\n",
				cid.NewCidV1(cid.Raw, digest), obj.ChunkLog,
				base64.StdEncoding.EncodeToString(blake3tree.Outboard(obj.Blocks, obj.Size)))
			return nil
		},
	}
	c.Flags().BoolVar(&bao, "bao", false, "also print the ChunkLog and the base64 Bao Outboard, as the Blake3 attribute carries them")
	c.Flags().Uint8Var(&chunkLog, "chunk-log", 0, "with --bao: build the outboard at this block size (base-2 exponent of chunks) instead of ingot's; 4 for iroh")
	return c
}

// blake3TreeFlags are the Blake3 attribute's values a ranged check needs:
// the chunk log, and the outboard either inline as the base64 from the XML
// or from a file.
type blake3TreeFlags struct {
	outboard     string
	outboardPath string
	chunkLog     uint8
	chunkLogSet  bool
}

func (f *blake3TreeFlags) bind(c *cobra.Command) {
	c.Flags().StringVar(&f.outboard, "outboard", "", "the attribute's Outboard as its base64 string")
	c.Flags().StringVar(&f.outboardPath, "outboard-file", "", "file holding the attribute's Outboard, as the base64 string or as raw bytes")
	c.Flags().Uint8Var(&f.chunkLog, "chunk-log", 0, "the attribute's ChunkLog: block size as a base-2 exponent of 1 KiB chunks")
	c.MarkFlagsMutuallyExclusive("outboard", "outboard-file")
	c.PreRun = func(c *cobra.Command, _ []string) { f.chunkLogSet = c.Flags().Changed("chunk-log") }
}

// given reports whether any of the flags was set.
func (f *blake3TreeFlags) given() bool {
	return f.outboard != "" || f.outboardPath != "" || f.chunkLogSet
}

// load decodes and checks the outboard. The chunk log (0 is a valid value,
// the original Bao chunk, so it must be given explicitly) and one of the
// outboard flags are required together.
func (f *blake3TreeFlags) load() ([]byte, error) {
	if (f.outboard == "" && f.outboardPath == "") || !f.chunkLogSet {
		return nil, errors.New("--chunk-log and one of --outboard or --outboard-file are required together")
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
	// The chunk log and the outboard must agree with each other (and the
	// chunk log must be one a block size can be computed from) before
	// either is used.
	if _, err := blake3tree.OutboardBlocks(outboard, f.chunkLog); err != nil {
		return nil, fmt.Errorf("%s with --chunk-log %d: %w", src, f.chunkLog, err)
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

Without flags the whole object is expected and hashed. With --chunk-log,
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
			n, err := blake3tree.VerifyBlocks(in, outboard, tree.chunkLog, offset, root)
			var bad *blake3tree.BlockError
			if errors.As(err, &bad) {
				return &mismatchError{err.Error()}
			}
			if err != nil {
				return err
			}
			if n == 0 {
				fmt.Fprintf(cmd.OutOrStdout(), "ok empty object %s\n", want)
				return nil
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
			start, end, err := blake3tree.AlignedRange(a, b, tree.chunkLog, blake3tree.OutboardSize(outboard))
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
