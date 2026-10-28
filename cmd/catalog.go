package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"sort"
	"time"

	"github.com/fil-forge/hilt/pkg/sigv4"
	"github.com/fil-forge/libforge/commands/s3"
	ucanlib "github.com/fil-forge/libforge/ucan"
	"github.com/fil-forge/ucantone/did"
	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/fil-forge/ingot"
	"github.com/fil-forge/ingot/blockstore"
	"github.com/fil-forge/ingot/cars"
	"github.com/fil-forge/ingot/config"
	"github.com/fil-forge/ingot/iam"
	"github.com/fil-forge/ingot/internal/reqscope"
	"github.com/fil-forge/ingot/internal/tracing"
	"github.com/fil-forge/ingot/logstore"
	"github.com/fil-forge/ingot/registry"
)

func newCatalogCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "catalog",
		Short: "Inspect the buckets' catalogs",
	}
	c.AddCommand(newCatalogUsageCmd())
	return c
}

func newCatalogUsageCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "usage",
		Short: "Print each bucket's catalog storage on Forge, dead on arrival and live, as JSON",
		Long: `Print each bucket's catalog storage as JSON, labeled by the bucket's space
DID, and the totals over every bucket:

  forge_bytes            shipped catalog segments, retired ones included
  dead_on_arrival_bytes  their blocks already dead when they shipped
  unmeasured_bytes       shipped segments with no dead-on-arrival figure
  local_bytes            the catalog segment files on this node's disk
  live_bytes             blocks the bucket's root reaches (their CAR frames;
                         the others above count whole files)
  live_blocks            how many such blocks
  missing_blocks         catalog blocks the root reaches that the walk could
                         not read; any makes live_bytes a lower bound
  fetched_segments       shipped segments the walk read from Forge

The first three come from Postgres. Dead after shipping is roughly
forge_bytes - live_bytes - dead_on_arrival_bytes: CAR headers count only in
forge_bytes, blocks of unshipped segments count as live, and a block can count
both as live and as dead on arrival in an older segment.

The walk from each bucket's root reads the bucket's local catalog segments.
A block they no longer hold, such as one of a retired segment, is read with
the rest of its shipped segment from Forge, with the authority of the access
key in AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY: Hilt authorizes a
ListObjectsV2 of the bucket signed with that key, as it does one the gateway
serves. Without the key such blocks count as missing, and so do those of a
bucket the key may not list. The walk holds every segment it reads in memory,
so the command suits a test stack, such as a benchmark run, rather than a
production node. It only reads, and can run beside a live daemon.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := config.Load(cfgFile)
			if err != nil {
				return err
			}
			if cfg.DataDir == "" || cfg.PostgresDSN == "" {
				return errors.New("catalog usage needs data_dir and postgres_dsn")
			}
			key, withKey, err := accessKeyFromEnv()
			if err != nil {
				return err
			}
			pool, err := openPool(cmd.Context(), cfg.PostgresDSN)
			if err != nil {
				return err
			}
			defer pool.Close()
			pg := registry.NewPostgres(pool)
			var segments segmentSource
			if withKey {
				if segments, err = newForgeSegments(*cfg, pg, key); err != nil {
					return err
				}
			}
			report, err := catalogUsage(cmd.Context(), pg, ingot.SegmentsDir(cfg.DataDir), segments)
			if err != nil {
				return err
			}
			enc := json.NewEncoder(cmd.OutOrStdout())
			enc.SetIndent("", "  ")
			return enc.Encode(report)
		},
	}
}

// catalogFigures are one bucket's catalog measurements, or their sum over
// every bucket. The command's help text defines each.
type catalogFigures struct {
	ForgeBytes         int64 `json:"forge_bytes"`
	DeadOnArrivalBytes int64 `json:"dead_on_arrival_bytes"`
	UnmeasuredBytes    int64 `json:"unmeasured_bytes"`
	LocalBytes         int64 `json:"local_bytes"`
	LiveBytes          int64 `json:"live_bytes"`
	LiveBlocks         int64 `json:"live_blocks"`
	MissingBlocks      int64 `json:"missing_blocks"`
	FetchedSegments    int64 `json:"fetched_segments"`
}

func (f *catalogFigures) add(o catalogFigures) {
	f.ForgeBytes += o.ForgeBytes
	f.DeadOnArrivalBytes += o.DeadOnArrivalBytes
	f.UnmeasuredBytes += o.UnmeasuredBytes
	f.LocalBytes += o.LocalBytes
	f.LiveBytes += o.LiveBytes
	f.LiveBlocks += o.LiveBlocks
	f.MissingBlocks += o.MissingBlocks
	f.FetchedSegments += o.FetchedSegments
}

// bucketCatalogUsage is one bucket's figures. The bucket goes by its space
// DID: its name is customer data.
type bucketCatalogUsage struct {
	Space string `json:"space"`
	catalogFigures
}

type catalogUsageReport struct {
	Buckets []bucketCatalogUsage `json:"buckets"`
	Total   catalogFigures       `json:"total"`
}

// catalogUsageSource is what the report reads from Postgres.
type catalogUsageSource interface {
	registry.CatalogUsageStore
	Get(ctx context.Context, name string) (*registry.State, error)
}

// segmentSource gives the walk of a bucket a way to read the bucket's
// shipped catalog segments from Forge.
type segmentSource func(bucket string, space did.DID) logstore.FetchSegment

// catalogUsage measures every bucket that has shipped catalog segments or a
// catalog log under segmentsDir. A bucket whose row is gone (deleted, with
// its log not yet removed) is left out. With segments nil, the walk reads
// local segments alone.
func catalogUsage(ctx context.Context, src catalogUsageSource, segmentsDir string, segments segmentSource) (catalogUsageReport, error) {
	report := catalogUsageReport{Buckets: []bucketCatalogUsage{}}
	shipped, err := src.CatalogUsage(ctx)
	if err != nil {
		return report, err
	}
	byName := make(map[string]registry.CatalogUsage, len(shipped))
	for _, u := range shipped {
		byName[u.Bucket] = u
	}
	names := make(map[string]struct{}, len(shipped))
	for name := range byName {
		names[name] = struct{}{}
	}
	entries, err := os.ReadDir(segmentsDir)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return report, fmt.Errorf("list %s: %w", segmentsDir, err)
	}
	for _, e := range entries {
		if e.IsDir() {
			names[e.Name()] = struct{}{}
		}
	}

	for name := range names {
		st, err := src.Get(ctx, name)
		if errors.Is(err, registry.ErrNotFound) {
			continue
		}
		if err != nil {
			return report, fmt.Errorf("bucket %q: %w", name, err)
		}
		var fetch logstore.FetchSegment
		if segments != nil {
			fetch = segments(name, st.Space)
		}
		walk, err := logstore.WalkCatalog(ctx, segmentsDir, name, st.Root, fetch)
		if err != nil {
			return report, fmt.Errorf("bucket %q: %w", name, err)
		}
		u := byName[name]
		b := bucketCatalogUsage{
			Space: st.Space.String(),
			catalogFigures: catalogFigures{
				ForgeBytes:         u.Bytes,
				DeadOnArrivalBytes: u.DeadOnArrivalBytes,
				UnmeasuredBytes:    u.UnmeasuredBytes,
				LocalBytes:         walk.FileBytes,
				LiveBytes:          walk.LiveBytes,
				LiveBlocks:         walk.LiveBlocks,
				MissingBlocks:      walk.MissingBlocks,
				FetchedSegments:    walk.FetchedSegments,
			},
		}
		report.Buckets = append(report.Buckets, b)
		report.Total.add(b.catalogFigures)
	}
	sort.Slice(report.Buckets, func(i, j int) bool { return report.Buckets[i].Space < report.Buckets[j].Space })
	return report, nil
}

// accessKey is an S3 access key and its secret.
type accessKey struct {
	id, secret string
}

// accessKeyFromEnv reads the access key the walk reads Forge with from the
// AWS SDKs' own variables, so the secret never appears in the command line.
func accessKeyFromEnv() (accessKey, bool, error) {
	key := accessKey{id: os.Getenv("AWS_ACCESS_KEY_ID"), secret: os.Getenv("AWS_SECRET_ACCESS_KEY")}
	switch {
	case key.id == "" && key.secret == "":
		return key, false, nil
	case key.id == "" || key.secret == "":
		return key, false, errors.New("set both AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY, or neither")
	}
	return key, true, nil
}

// signedAuthorizer is the slice of [iam.Service] forgeSegments uses.
type signedAuthorizer interface {
	AuthorizeSigned(ctx context.Context, req s3.Request, accessKey string) (*iam.DelegationCache, error)
}

// blobOpener is the slice of [blockstore.Forge] forgeSegments reads with.
type blobOpener interface {
	OpenBlob(ctx context.Context, space did.DID, digest mh.Multihash) (io.ReadCloser, error)
}

// forgeSegments reads buckets' shipped catalog segments from Forge with an
// access key's authority over them.
type forgeSegments struct {
	authz      signedAuthorizer
	blobs      blobOpener
	inclusions registry.InclusionReader
	key        accessKey
	region     string
	logger     *zap.Logger
}

// newForgeSegments builds the readers the daemon reads Forge with: the Hilt
// client and the network read tier, as the agent identity, over the local
// locator.
func newForgeSegments(cfg config.Config, pg *registry.Postgres, key accessKey) (segmentSource, error) {
	logger, err := buildLogger(cfg.LogLevel)
	if err != nil {
		return nil, err
	}
	id, err := loadAgentIdentity(cfg.Identity)
	if err != nil {
		return nil, err
	}
	client, err := ingot.NewAuthServiceClient(cfg, id, logger)
	if err != nil {
		return nil, err
	}
	forge, err := blockstore.NewForge(blockstore.ForgeConfig{
		Locator:    registry.NewLocalLocator(pg, pg),
		Signer:     id,
		HTTPClient: tracing.NewHTTPClient(),
		Logger:     logger,
	})
	if err != nil {
		return nil, err
	}
	region := cfg.Region
	if region == "" {
		region = config.DefaultRegion
	}
	f := &forgeSegments{
		authz:      iam.New(client, iam.NewKeyProofs(), iam.NewVerificationKeyCache(), iam.NewTenantCache(), iam.WithLogger(logger)),
		blobs:      forge,
		inclusions: pg,
		key:        key,
		region:     region,
		logger:     logger,
	}
	return f.forBucket, nil
}

// forBucket fetches bucket's shipped segments. It asks Hilt for the key's
// authority over the bucket when the walk first needs a segment, so a bucket
// whose blocks are all on disk costs no call. A bucket the key may not list
// is logged once and fetched from no further.
func (f *forgeSegments) forBucket(bucket string, space did.DID) logstore.FetchSegment {
	var (
		store   ucanlib.ProofStore
		refused bool
		fetched = make(map[string]struct{})
	)
	return func(ctx context.Context, c cid.Cid) ([]block.Block, error) {
		if refused {
			return nil, nil
		}
		inc, err := f.inclusions.GetInclusion(ctx, space, c.Hash())
		if errors.Is(err, registry.ErrNotFound) {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		shard := inc.ShardDigest
		if _, ok := fetched[string(shard)]; ok {
			return nil, nil
		}
		if store == nil {
			req, err := signedList(bucket, f.key, f.region, time.Now())
			if err != nil {
				return nil, err
			}
			s, err := f.authz.AuthorizeSigned(ctx, req, f.key.id)
			if errors.Is(err, iam.ErrBucketNotPermitted) {
				refused = true
				f.logger.Warn("catalog usage: the access key may not list the bucket, so its blocks off this disk count as missing",
					zap.Stringer("space", space), zap.Error(err))
				return nil, nil
			}
			if err != nil {
				return nil, err
			}
			store = s
		}
		fetched[string(shard)] = struct{}{}
		rc, err := f.blobs.OpenBlob(context.WithValue(ctx, reqscope.ProofStoreKey(), store), space, shard)
		if err != nil {
			return nil, err
		}
		defer rc.Close()
		body, err := io.ReadAll(rc)
		if err != nil {
			return nil, fmt.Errorf("read segment %s: %w", shard.B58String(), err)
		}
		// Neither the read tier nor the block constructor checks the bytes,
		// and a segment cut short at a frame boundary would parse with fewer
		// blocks, so the segment is checked whole against its digest.
		if err := matchesDigest(body, shard); err != nil {
			return nil, err
		}
		res, err := cars.Scan(bytes.NewReader(body), int64(len(body)))
		if err != nil {
			return nil, fmt.Errorf("parse segment %s: %w", shard.B58String(), err)
		}
		blocks := make([]block.Block, len(res.Frames))
		for i, fr := range res.Frames {
			blocks[i] = fr.Block
		}
		return blocks, nil
	}
}

// matchesDigest reports whether body hashes to digest.
func matchesDigest(body []byte, digest mh.Multihash) error {
	dec, err := mh.Decode(digest)
	if err != nil {
		return fmt.Errorf("segment digest %x: %w", []byte(digest), err)
	}
	sum, err := mh.Sum(body, dec.Code, dec.Length)
	if err != nil {
		return fmt.Errorf("hash segment %s: %w", digest.B58String(), err)
	}
	if !bytes.Equal(sum, digest) {
		return fmt.Errorf("segment %s read from Forge does not match its digest", digest.B58String())
	}
	return nil
}

// listHost is the host the signed ListObjectsV2 names. The request is only
// shown to Hilt, never sent, so the host need only be one the signature
// covers; .invalid names nothing (RFC 2606).
const listHost = "ingot.invalid"

// signedList is a ListObjectsV2 of bucket, presigned with key, in the shape
// the gateway shows Hilt a request it serves: the request URI and the Host
// header. s3:ListBucket, the permission it needs, maps to /content/retrieve
// over the bucket's space, which is what reading its segments takes.
//
// Hilt refuses a presigned request dated after its own clock, with no skew,
// so the request is dated sigv4.MaxClockSkew before now and valid for twice
// that: either clock may lead by the skew Hilt allows a header-signed one.
func signedList(bucket string, key accessKey, region string, now time.Time) (s3.Request, error) {
	u := url.URL{Scheme: "http", Host: listHost, Path: "/" + bucket, RawQuery: "list-type=2"}
	signed, err := sigv4.Presign(sigv4.Request{Method: http.MethodGet, URL: u.String()},
		key.id, key.secret, region, sigv4.SchemeV4, now.Add(-sigv4.MaxClockSkew), 2*sigv4.MaxClockSkew)
	if err != nil {
		return s3.Request{}, fmt.Errorf("sign a listing of bucket %q: %w", bucket, err)
	}
	su, err := url.Parse(signed.URL)
	if err != nil {
		return s3.Request{}, fmt.Errorf("sign a listing of bucket %q: %w", bucket, err)
	}
	return s3.Request{Method: signed.Method, URL: su.RequestURI(), Headers: map[string]string{"Host": su.Host}}, nil
}
