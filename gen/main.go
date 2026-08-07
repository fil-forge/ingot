// Generates CBOR marshal/unmarshal methods for ingot types. Invoked via
// `go generate ./...` (or `make gen`); the directive below runs this program
// from the gen/ directory, so output paths are relative to it.

//go:generate go run .

package main

import (
	"github.com/fil-forge/ingot/bucket"
	cbg "github.com/whyrusleeping/cbor-gen"
)

func main() {
	cfg := cbg.Gen{MaxStringLength: 1_000_000}
	if err := cfg.WriteMapEncodersToFile("../bucket/cbor_gen.go", "bucket",
		bucket.ObjectManifest{},
		bucket.Body{},
		bucket.BlobRef{},
		bucket.ObjectLeaf{},
		bucket.VersionNode{},
		bucket.ValueUnion{},
	); err != nil {
		panic(err)
	}
}
