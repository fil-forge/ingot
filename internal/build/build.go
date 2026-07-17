// Package build carries version metadata stamped into the ingot binary at
// release time. goreleaser sets these via -ldflags -X (see .goreleaser.yaml);
// in a plain `go build` they are empty/"unknown" and we fall back to the
// module's VCS build info so dev binaries still self-identify.
package build

import "runtime/debug"

// Set at release time by goreleaser via -ldflags -X; see .goreleaser.yaml.
var (
	// Version is the release version, e.g. "v0.1.0" ("dev" when unstamped).
	Version string
	// Commit is the git commit the binary was built from.
	Commit = "unknown"
	// Date is the commit/build date (RFC3339, UTC).
	Date = "unknown"
	// BuiltBy names what produced the binary (e.g. "goreleaser").
	BuiltBy = "unknown"
)

func init() {
	if Version == "" {
		Version = "dev"
	}
	// For unstamped (dev) builds, recover commit/date from the embedded VCS
	// build info Go records automatically.
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, s := range info.Settings {
			switch s.Key {
			case "vcs.revision":
				if Commit == "unknown" {
					Commit = s.Value
				}
			case "vcs.time":
				if Date == "unknown" {
					Date = s.Value
				}
			}
		}
	}
}
