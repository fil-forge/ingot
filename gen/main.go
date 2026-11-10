// Generates CBOR marshal/unmarshal methods for ingot types. Invoked via
// `go generate ./...` (or `make gen`); the directive below runs this program
// from the gen/ directory, so output paths are relative to it.

//go:generate go run .

package main

import (
	"github.com/fil-forge/ingot/bucket"
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
		registry.UploadTask{},
		registry.ReleaseTask{},
	); err != nil {
		panic(err)
	}
	if err := cfg.WriteMapEncodersToFile("../s3frontend/cbor_gen.go", "s3frontend",
		s3frontend.ShardCacheTask{},
		s3frontend.ShardEvictTask{},
		s3frontend.ShardAddTask{},
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
