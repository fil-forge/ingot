package uploader

import (
	"context"
	"net/url"

	blobcmds "github.com/fil-forge/libforge/commands/blob"
	"github.com/fil-forge/ucantone/did"
)

// ProviderInfo identifies a storage provider (a piri node): its DID and the
// base URL its UCAN endpoint is served from.
type ProviderInfo struct {
	ID       did.DID
	Endpoint url.URL
}

// ProviderSelector chooses the storage provider a blob should be allocated to.
//
// This is the seam that, in the forge prototype, was sprue's
// routing.Service (backed by a network-wide storage-provider store). In the
// Forge co-located / standalone-client deployments ingot targets there is a
// single home piri, so the default StaticProviderSelector is usually enough; a
// host with a real routing service (e.g. sprue) supplies its own
// implementation.
type ProviderSelector interface {
	SelectStorageProvider(ctx context.Context, blob blobcmds.Blob) (ProviderInfo, error)
}

// StaticProviderSelector always returns the single configured home provider.
type StaticProviderSelector struct {
	Provider ProviderInfo
}

func (s StaticProviderSelector) SelectStorageProvider(ctx context.Context, _ blobcmds.Blob) (ProviderInfo, error) {
	return s.Provider, nil
}

var _ ProviderSelector = StaticProviderSelector{}

// NewStaticProviderSelector returns a ProviderSelector that always allocates to
// the single home piri at (id, endpoint). This is the common case for the
// co-located and standalone-client deployments; a host wires it into ingot's fx
// module with a constructor that returns the interface (so it registers under
// ProviderSelector rather than the concrete type — fx.Supply would key it by
// the concrete type):
//
//	fx.Provide(func() uploader.ProviderSelector {
//	    return uploader.NewStaticProviderSelector(piriDID, piriURL)
//	})
func NewStaticProviderSelector(id did.DID, endpoint url.URL) ProviderSelector {
	return StaticProviderSelector{Provider: ProviderInfo{ID: id, Endpoint: endpoint}}
}
