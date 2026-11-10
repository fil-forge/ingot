// Package preload loads an ingot gateway's catalog to estate scale through
// its S3 API, for measuring the catalog there (FIL-806). It is the library
// behind cmd/ingot-preload.
//
// A load writes objects the way the pack server does: each key is a prefix
// followed by 64 lowercase hex digits, and each PUT is create-only
// (If-None-Match: *) and carries the body's SHA-256 (x-amz-checksum-sha256).
// Most objects are zero-byte "bulk" objects, which run ingot's whole commit,
// seal and ship without uploading a blob. A few "real" objects per bucket
// carry pack-sized pseudo-random bodies, so that reads, deletes and the
// per-blob tables can be measured on objects shaped like production's.
//
// Everything a load writes follows from its seed: bulk object n of a bucket
// always has the same key, and real object j always has the same body. A
// load records how far each bucket got in a state file, and a later load
// resumes from there. A PUT the earlier load completed but did not record
// finds its key taken, and counts as written.
package preload
