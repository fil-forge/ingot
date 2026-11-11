// Generates CBOR marshal/unmarshal methods for ingot types. Invoked via
// `go generate ./...` (or `make gen`); the directive below runs this program
// from the gen/ directory, so output paths are relative to it.

//go:generate go run .

package main

import (
	"github.com/fil-forge/ingot/bucket"
	"github.com/fil-forge/ingot/queue/bucketqueue"
	"github.com/fil-forge/ingot/queue/multipartqueue"
	"github.com/fil-forge/ingot/queue/shardqueue"
	"github.com/fil-forge/ingot/queue/spaceshardqueue"
	"github.com/fil-forge/ingot/queue/spaceuploadqueue"
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
	if err := cfg.WriteMapEncodersToFile("../queue/spaceshardqueue/cbor_gen.go", "spaceshardqueue",
		spaceshardqueue.AddTask{},
		spaceshardqueue.ReleaseTask{},
	); err != nil {
		panic(err)
	}
	if err := cfg.WriteMapEncodersToFile("../queue/bucketqueue/cbor_gen.go", "bucketqueue",
		bucketqueue.BucketCommitTask{},
		bucketqueue.KeyChange{},
		bucketqueue.PutChange{},
		bucketqueue.BlobRefTask{},
		bucketqueue.ReservedVersion{},
		bucketqueue.DeleteChange{},
		bucketqueue.DeletePreconditions{},
		bucketqueue.SetAttributesChange{},
		bucketqueue.BucketCommitResult{},
		bucketqueue.ChangeResult{},
		bucketqueue.ChangeError{},
		bucketqueue.BucketTask{},
		bucketqueue.BucketCreateTask{},
		bucketqueue.BucketSetVersioningTask{},
		bucketqueue.BucketSetLockConfigTask{},
		bucketqueue.BucketSetConfigTask{},
	); err != nil {
		panic(err)
	}
	if err := cfg.WriteMapEncodersToFile("../queue/multipartqueue/cbor_gen.go", "multipartqueue",
		multipartqueue.AbortTask{},
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
