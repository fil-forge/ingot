// Carried from github.com/fil-forge/guppy/pkg/client/spaces.go.
package forgeclient

import "github.com/fil-forge/ucantone/ipld/datamodel"

// SpaceNameMetaKey is the delegation-metadata key under which a space's
// human-readable name is stored.
const SpaceNameMetaKey = "name"

// SpaceNameMetadata returns delegation metadata carrying the given space name.
func SpaceNameMetadata(name string) datamodel.Map {
	return datamodel.Map{SpaceNameMetaKey: name}
}
