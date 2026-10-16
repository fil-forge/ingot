package bucket

import (
	"github.com/ipfs/go-cid"
)

// ObjectManifestV0 is the "/objectmanifest/0" manifest: ObjectManifest as it
// was before the blob list moved to extents. Decode only. ObjectValue turns
// it into an ObjectManifest on read, so nothing past the decode sees it.
// Frozen: fields added to manifests go on ObjectManifest.
type ObjectManifestV0 struct {
	Key                     string            `cborgen:"k"`
	ContentType             string            `cborgen:"ct"`
	Created                 int64             `cborgen:"t"`
	Body                    BodyV0            `cborgen:"b"`
	ETag                    string            `cborgen:"e"`
	DeleteMarker            bool              `cborgen:"dm"`
	Seq                     uint64            `cborgen:"sq"`
	VersionID               string            `cborgen:"vi"`
	ChecksumAlgorithm       string            `cborgen:"ca"`
	Checksum                string            `cborgen:"ck"`
	ChecksumType            string            `cborgen:"cy"`
	ContentEncoding         string            `cborgen:"ce"`
	ContentDisposition      string            `cborgen:"cd"`
	ContentLanguage         string            `cborgen:"cl"`
	CacheControl            string            `cborgen:"cc"`
	Expires                 string            `cborgen:"ex"`
	WebsiteRedirectLocation string            `cborgen:"wr"`
	Metadata                map[string]string `cborgen:"md"`
	Plaintext               bool              `cborgen:"pt"`
}

// BodyV0 is the "/objectmanifest/0" body, whose blob list is inline and
// stores each blob's span. Decode only, frozen.
type BodyV0 struct {
	Size          int64     `cborgen:"s"`
	MD5           []byte    `cborgen:"m"`
	Blobs         []BlobRef `cborgen:"bl"`
	BLAKE3        []byte    `cborgen:"b3"`
	TreeChunkLog  uint8     `cborgen:"tc"`
	TreeBlocks    []byte    `cborgen:"tb"`
	PartSizes     []int64   `cborgen:"ps"`
	PartChecksums []string  `cborgen:"pc"`
	IndexRoot     *cid.Cid  `cborgen:"ir"`
}

// upgrade returns the manifest as an ObjectManifest. The blob list becomes
// Head; a /0 list holds at most 8,192 blobs, all inline.
func (m *ObjectManifestV0) upgrade() *ObjectManifest {
	b := m.Body
	head := make([]BlobEntry, len(b.Blobs))
	for i, ref := range b.Blobs {
		head[i] = BlobEntry{Digest: ref.Digest, Offset: ref.Start}
	}
	return &ObjectManifest{
		Key:          m.Key,
		ContentType:  m.ContentType,
		Created:      m.Created,
		ETag:         m.ETag,
		DeleteMarker: m.DeleteMarker,
		Seq:          m.Seq,
		VersionID:    m.VersionID,
		Body: Body{
			Size:          b.Size,
			MD5:           b.MD5,
			Blobs:         BlobList{Head: head},
			BLAKE3:        b.BLAKE3,
			TreeChunkLog:  b.TreeChunkLog,
			TreeBlocks:    b.TreeBlocks,
			PartSizes:     b.PartSizes,
			PartChecksums: b.PartChecksums,
			IndexRoot:     b.IndexRoot,
		},
		ChecksumAlgorithm:       m.ChecksumAlgorithm,
		Checksum:                m.Checksum,
		ChecksumType:            m.ChecksumType,
		ContentEncoding:         m.ContentEncoding,
		ContentDisposition:      m.ContentDisposition,
		ContentLanguage:         m.ContentLanguage,
		CacheControl:            m.CacheControl,
		Expires:                 m.Expires,
		WebsiteRedirectLocation: m.WebsiteRedirectLocation,
		Metadata:                m.Metadata,
		Plaintext:               m.Plaintext,
	}
}
