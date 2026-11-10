// Generates CBOR marshal/unmarshal methods for ingot types. Invoked via
// `go generate ./...` (or `make gen`); the directive below runs this program
// from the gen/ directory, so output paths are relative to it.

//go:generate go run .

package main

import (
	"github.com/fil-forge/ingot/bucket"
	"github.com/fil-forge/ingot/queue/shardqueue"
	"github.com/fil-forge/ingot/queue/spaceuploadqueue"
	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ingot/s3frontend"
	cbg "github.com/whyrusleeping/cbor-gen"
)

func main() {
	cfg := cbg.Gen{MaxStringLength: 1_000_000}
	if err := cfg.WriteMapEncodersToFile("../bucket/cbor_gen.go", "bucket",
		bucket.ObjectManifest{},
		bucket.Body{},
		bucket.BlobList{},
		bucket.BlobExtentUnion{},
		bucket.ObjectManifestV0{},
		bucket.BodyV0{},
		bucket.BlobRef{},
		bucket.ObjectLeaf{},
		bucket.VersionNode{},
		bucket.ValueUnion{},
		bucket.VersionState{},
		bucket.StateUnion{},
	); err != nil {
		panic(err)
	}
	if err := cfg.WriteMapEncodersToFile("../registry/cbor_gen.go", "registry",
		registry.ReleaseTask{},
	); err != nil {
		panic(err)
	}
	if err := cfg.WriteMapEncodersToFile("../queue/shardqueue/cbor_gen.go", "shardqueue",
		shardqueue.CacheTask{},
		shardqueue.EvictTask{},
	); err != nil {
		panic(err)
	}
	if err := cfg.WriteMapEncodersToFile("../queue/spaceuploadqueue/cbor_gen.go", "spaceuploadqueue",
		spaceuploadqueue.Task{},
	); err != nil {
		panic(err)
	}
	if err := cfg.WriteMapEncodersToFile("../s3frontend/cbor_gen.go", "s3frontend",
		s3frontend.ShardAddTask{},
		s3frontend.BucketCommitTask{},
		s3frontend.KeyChange{},
		s3frontend.PutChange{},
		s3frontend.BlobRefTask{},
		s3frontend.ReservedVersion{},
		s3frontend.DeleteChange{},
		s3frontend.DeletePreconditions{},
		s3frontend.SetAttributesChange{},
		s3frontend.BucketCommitResult{},
		s3frontend.ChangeResult{},
		s3frontend.ChangeError{},
		s3frontend.BucketTask{},
		s3frontend.BucketCreateTask{},
		s3frontend.BucketSetVersioningTask{},
		s3frontend.BucketSetLockConfigTask{},
		s3frontend.BucketSetConfigTask{},
		s3frontend.MultipartAbortTask{},
	); err != nil {
		panic(err)
	}
	// Tuple-encoded: a blob list's entries, and an extent as a bare array of
	// them (cbor-gen's transparent tag works in tuple mode only).
	if err := cfg.WriteTupleEncodersToFile("../bucket/cbor_gen_tuple.go", "bucket",
		bucket.BlobEntry{},
		bucket.ExtentRef{},
		bucket.BlobExtent{},
	); err != nil {
		panic(err)
	}
}
