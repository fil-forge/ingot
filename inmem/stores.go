package inmem

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ucantone/did"
	"github.com/multiformats/go-multihash"
)

// In-memory implementations of the architecture's relational stores
// (registry.BlobRefStore / IntentStore / LocationStore / EncryptionParamsStore /
// MultipartStore / GCStore), mirroring the Postgres tables so the in-process
// suite and standalone mode exercise the same write/read/delete code paths.

// Compile-time assertions: *MemStore satisfies every store interface.
var (
	_ registry.BlobRefStore          = (*MemStore)(nil)
	_ registry.IntentStore           = (*MemStore)(nil)
	_ registry.LocationStore         = (*MemStore)(nil)
	_ registry.EncryptionParamsStore = (*MemStore)(nil)
	_ registry.InclusionStore        = (*MemStore)(nil)
	_ registry.MultipartStore        = (*MemStore)(nil)
	_ registry.GCStore               = (*MemStore)(nil)
	_ registry.RevocationCursorStore = (*MemStore)(nil)
	_ registry.PendingReleaseStore   = (*MemStore)(nil)
)

// BlobRefStore ===============================================================
//
// The index is written by CommitRoot (store.go) alone; the helpers below are
// its steps, under the mutex.

// claimRefusedLocked is the check a claim of a new generation passes before
// a commit records it: ErrReleaseInProgress while a started release holds
// the digest, or when the space has no location for it (see
// registry.RootCommit.Claims).
func (m *MemStore) claimRefusedLocked(c registry.BlobClaim) error {
	if pr, ok := m.releases[locKey{c.Space, string(c.Digest)}]; ok && !pr.StartedAt.IsZero() {
		return registry.ErrReleaseInProgress
	}
	if _, ok := m.locations[locKey{c.Space, string(c.Digest)}]; !ok {
		return registry.ErrReleaseInProgress // released to completion since the write settled it
	}
	return nil
}

// pinRefusedLocked reports whether a pinned claim finds no claim of the
// space on its digest to join (see registry.RootCommit.Pins). A pin already
// recorded is its own claim.
func (m *MemStore) pinRefusedLocked(c registry.BlobClaim) bool {
	if _, ok := m.blobRefs[claimKey{string(c.Digest), c.Bucket, c.ObjectKey, c.VersionID}]; ok {
		return false
	}
	return m.countClaimsLocked(c.Space, c.Digest) == 0
}

// addClaimLocked records one claim. A new generation's claim publishes the
// digest's upload intent, atomically with the claim; a pin joins a claim
// that did so already.
func (m *MemStore) addClaimLocked(c registry.BlobClaim, publish bool) {
	k := claimKey{string(c.Digest), c.Bucket, c.ObjectKey, c.VersionID}
	cp := c
	cp.Digest = bytes.Clone(c.Digest)
	m.blobRefs[k] = cp
	if !publish {
		return
	}
	ik := locKey{c.Space, string(c.Digest)}
	if in, ok := m.intents[ik]; ok && in.State != registry.IntentPublished {
		in.State = registry.IntentPublished
		in.UpdatedAt = time.Now()
		m.intents[ik] = in
	}
}

// dropClaimLocked deletes one claim and records the blob's release, due at
// due, when the claim was the space's last on the digest (see
// registry.RootCommit.Drops).
func (m *MemStore) dropClaimLocked(d registry.ClaimDrop, due time.Time) {
	delete(m.blobRefs, claimKey{string(d.Digest), d.Bucket, d.ObjectKey, d.VersionID})
	if m.countClaimsLocked(d.Space, d.Digest) != 0 {
		return
	}
	m.enqueueReleaseLocked(d.Space, d.Digest, due)
}

func (m *MemStore) CountClaims(_ context.Context, space did.DID, digest multihash.Multihash) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.countClaimsLocked(space, digest), nil
}

func (m *MemStore) countClaimsLocked(space did.DID, digest multihash.Multihash) int {
	d := string(digest)
	n := 0
	for k, c := range m.blobRefs {
		if k.digest == d && c.Space == space {
			n++
		}
	}
	return n
}

// PendingReleaseStore =========================================================

func (m *MemStore) EnqueueRelease(_ context.Context, space did.DID, digest multihash.Multihash, notBefore time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.enqueueReleaseLocked(space, digest, notBefore)
	return nil
}

func (m *MemStore) enqueueReleaseLocked(space did.DID, digest multihash.Multihash, notBefore time.Time) registry.PendingRelease {
	k := locKey{space, string(digest)}
	if prior, ok := m.releases[k]; ok && prior.NotBefore.After(notBefore) {
		notBefore = prior.NotBefore // upsert keeps the later not_before
	}
	pr := registry.PendingRelease{
		Space: space, Digest: multihash.Multihash(bytes.Clone(digest)), NotBefore: notBefore,
	}
	if prior, ok := m.releases[k]; ok {
		pr.StartedAt = prior.StartedAt // a started record stays started
	}
	m.releases[k] = pr
	return pr
}

func (m *MemStore) EnqueueReleases(_ context.Context, space did.DID, digests []multihash.Multihash, notBefore time.Time) ([]registry.PendingRelease, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	seen := map[string]bool{}
	var out []registry.PendingRelease
	for _, d := range digests {
		if seen[string(d)] {
			continue
		}
		seen[string(d)] = true
		out = append(out, m.enqueueReleaseLocked(space, d, notBefore))
	}
	return out, nil
}

func (m *MemStore) ListDueReleases(_ context.Context, now time.Time, limit int) ([]registry.PendingRelease, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []registry.PendingRelease
	for _, pr := range m.releases {
		if !pr.NotBefore.After(now) {
			out = append(out, pr)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].NotBefore.Before(out[j].NotBefore) })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (m *MemStore) ListReleasesBySpace(_ context.Context, space did.DID) ([]registry.PendingRelease, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []registry.PendingRelease
	for k, pr := range m.releases {
		if k.space == space {
			out = append(out, pr)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].NotBefore.Before(out[j].NotBefore) })
	return out, nil
}

func (m *MemStore) DeleteIntentAndRelease(_ context.Context, space did.DID, digest multihash.Multihash) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deleteIntentLocked(locKey{space, string(digest)})
	delete(m.releases, locKey{space, string(digest)})
	return m.countIntentsLocked(string(digest), nil), nil
}

// deleteIntentLocked removes one intent and, with it the last for its
// digest, the digest's evicted mark.
func (m *MemStore) deleteIntentLocked(k locKey) {
	delete(m.intents, k)
	delete(m.evicted, k)
}

// countIntentsLocked returns how many intents name digest, in every space
// but except (nil counts them all).
func (m *MemStore) countIntentsLocked(digest string, except *did.DID) int {
	n := 0
	for k := range m.intents {
		if k.digest == digest && (except == nil || k.space != *except) {
			n++
		}
	}
	return n
}

func (m *MemStore) DeleteRelease(_ context.Context, space did.DID, digest multihash.Multihash) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.releases, locKey{space, string(digest)})
	return nil
}

// IntentStore ================================================================

func (m *MemStore) PutIntent(_ context.Context, in registry.UploadIntent) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := in
	cp.Digest = bytes.Clone(in.Digest)
	cp.UpdatedAt = time.Now()
	m.intents[locKey{in.Space, string(in.Digest)}] = cp
	// The file is back on disk, for every space's intent.
	for k := range m.evicted {
		if k.digest == string(in.Digest) {
			delete(m.evicted, k)
		}
	}
	return nil
}

func (m *MemStore) SetIntentState(_ context.Context, space did.DID, digest multihash.Multihash, state string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := locKey{space, string(digest)}
	in, ok := m.intents[k]
	if !ok {
		return registry.ErrNotFound
	}
	if in.State != state {
		in.State = state
		in.UpdatedAt = time.Now()
	}
	m.intents[k] = in
	return nil
}

func (m *MemStore) GetIntent(_ context.Context, space did.DID, digest multihash.Multihash) (*registry.UploadIntent, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	in, ok := m.intents[locKey{space, string(digest)}]
	if !ok {
		return nil, registry.ErrNotFound
	}
	cp := in
	cp.Digest = bytes.Clone(in.Digest)
	return &cp, nil
}

func (m *MemStore) CountOtherIntents(_ context.Context, space did.DID, digest multihash.Multihash) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.countIntentsLocked(string(digest), &space), nil
}

func (m *MemStore) ListIntentsByState(_ context.Context, state string) ([]registry.UploadIntent, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []registry.UploadIntent
	for _, in := range m.intents {
		if in.State != state {
			continue
		}
		cp := in
		cp.Digest = bytes.Clone(in.Digest)
		out = append(out, cp)
	}
	return out, nil
}

func (m *MemStore) DeleteIntent(_ context.Context, space did.DID, digest multihash.Multihash) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deleteIntentLocked(locKey{space, string(digest)})
	return nil
}

func (m *MemStore) ListEvictable(_ context.Context, after registry.EvictCursor, limit int) ([]registry.UploadIntent, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []registry.UploadIntent
	for key, in := range m.intents {
		if _, gone := m.evicted[key]; gone {
			continue
		}
		switch in.State {
		case registry.IntentParked:
			if _, ok := m.parks[key]; !ok {
				continue
			}
		case registry.IntentAccepted, registry.IntentPublished:
			if _, ok := m.locations[key]; !ok {
				continue
			}
		default:
			continue
		}
		if m.uploadingAnywhereLocked(key.digest) || !evictCursorBefore(after, in) {
			continue
		}
		cp := in
		cp.Digest = bytes.Clone(in.Digest)
		out = append(out, cp)
	}
	sort.Slice(out, func(i, j int) bool {
		return evictCursorBefore(registry.EvictCursor{UpdatedAt: out[i].UpdatedAt, Digest: out[i].Digest, Space: out[i].Space}, out[j])
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// uploadingAnywhereLocked reports whether any space's intent for digest is
// still spooled or uploading: an upload that reads the file.
func (m *MemStore) uploadingAnywhereLocked(digest string) bool {
	for k, in := range m.intents {
		if k.digest == digest && (in.State == registry.IntentSpooled || in.State == registry.IntentUploading) {
			return true
		}
	}
	return false
}

// locatedLocked reports whether digest has a location in any space.
func (m *MemStore) locatedLocked(digest multihash.Multihash) bool {
	for k := range m.locations {
		if k.digest == string(digest) {
			return true
		}
	}
	return false
}

// evictCursorBefore reports whether c sorts strictly before in in
// ListEvictable's (updated_at, digest, space) order.
func evictCursorBefore(c registry.EvictCursor, in registry.UploadIntent) bool {
	if !c.UpdatedAt.Equal(in.UpdatedAt) {
		return c.UpdatedAt.Before(in.UpdatedAt)
	}
	if d := bytes.Compare(c.Digest, in.Digest); d != 0 {
		return d < 0
	}
	return c.Space.String() < in.Space.String()
}

func (m *MemStore) MarkEvicted(_ context.Context, digest multihash.Multihash, notAfter time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.countIntentsLocked(string(digest), nil) == 0 {
		return registry.ErrNotFound
	}
	for k, in := range m.intents {
		if k.digest == string(digest) && !in.UpdatedAt.After(notAfter) {
			m.evicted[k] = struct{}{}
		}
	}
	return nil
}

// IsEvicted reports whether every intent for digest is marked evicted, and
// there is at least one. Test-only visibility into the evicted_at column.
func (m *MemStore) IsEvicted(digest multihash.Multihash) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for k := range m.intents {
		if k.digest != string(digest) {
			continue
		}
		if _, ok := m.evicted[k]; !ok {
			return false
		}
		n++
	}
	return n > 0
}

func (m *MemStore) ListReleasedPublished(_ context.Context, after multihash.Multihash, limit int) ([]registry.UploadIntent, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []registry.UploadIntent
	for key, in := range m.intents {
		if bytes.Compare(in.Digest, after) <= 0 || !m.releasedPublishedLocked(key, in) {
			continue
		}
		cp := in
		cp.Digest = bytes.Clone(in.Digest)
		out = append(out, cp)
	}
	sort.Slice(out, func(i, j int) bool { return bytes.Compare(out[i].Digest, out[j].Digest) < 0 })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (m *MemStore) DeleteReleasedPublished(_ context.Context, space did.DID, digest multihash.Multihash) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := locKey{space, string(digest)}
	in, ok := m.intents[key]
	if !ok || !m.releasedPublishedLocked(key, in) {
		return false, nil
	}
	m.deleteIntentLocked(key)
	return true, nil
}

// releasedPublishedLocked is ListReleasedPublished's condition: a published
// intent nothing names any more, in any space, and the digest's only intent.
func (m *MemStore) releasedPublishedLocked(key locKey, in registry.UploadIntent) bool {
	return in.State == registry.IntentPublished && !m.locatedLocked(in.Digest) &&
		!m.claimedAnywhereLocked(key.digest) && !m.releasePendingAnywhereLocked(key.digest) &&
		m.countLivePartRefsLocked(nil, in.Digest) == 0 &&
		m.countIntentsLocked(key.digest, &key.space) == 0
}

// claimedAnywhereLocked reports whether any blob_refs row, in any space,
// names the digest.
func (m *MemStore) claimedAnywhereLocked(digest string) bool {
	for k := range m.blobRefs {
		if k.digest == digest {
			return true
		}
	}
	return false
}

// releasePendingAnywhereLocked reports whether a release of the digest is
// pending in any space.
func (m *MemStore) releasePendingAnywhereLocked(digest string) bool {
	for k := range m.releases {
		if k.digest == digest {
			return true
		}
	}
	return false
}

// AgeIntent moves the last state change of every intent for digest back by
// d, as if it had happened that much earlier. Test-only: no store method
// rewrites history.
func (m *MemStore) AgeIntent(digest multihash.Multihash, d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for k, in := range m.intents {
		if k.digest == string(digest) {
			in.UpdatedAt = in.UpdatedAt.Add(-d)
			m.intents[k] = in
		}
	}
}

func (m *MemStore) StalledBytes(_ context.Context, before time.Time) (registry.StalledSizes, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var s registry.StalledSizes
	for _, in := range m.intents {
		if !in.UpdatedAt.Before(before) {
			continue
		}
		switch in.State {
		case registry.IntentSpooled:
			s.Spooled += in.Size
		case registry.IntentUploading:
			s.Uploading += in.Size
		}
	}
	return s, nil
}

func (m *MemStore) MissingIntents(_ context.Context, digests []multihash.Multihash) ([]multihash.Multihash, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []multihash.Multihash
	for _, d := range digests {
		if m.countIntentsLocked(string(d), nil) == 0 {
			out = append(out, bytes.Clone(d))
		}
	}
	return out, nil
}

// LocationStore ==============================================================

func (m *MemStore) PutLocation(_ context.Context, loc registry.BlobLocation) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.locations[locKey{loc.Space, string(loc.Digest)}] = cloneLocation(loc)
	return nil
}

func (m *MemStore) GetLocation(_ context.Context, space did.DID, digest multihash.Multihash) (*registry.BlobLocation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	loc, ok := m.locations[locKey{space, string(digest)}]
	if !ok {
		return nil, registry.ErrNotFound
	}
	cp := cloneLocation(loc)
	return &cp, nil
}

func (m *MemStore) GetLocations(_ context.Context, space did.DID, digests []multihash.Multihash) (map[string]registry.BlobLocation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]registry.BlobLocation, len(digests))
	for _, d := range digests {
		if loc, ok := m.locations[locKey{space, string(d)}]; ok {
			out[string(d)] = cloneLocation(loc)
		}
	}
	return out, nil
}

func (m *MemStore) DeleteLocation(_ context.Context, space did.DID, digest multihash.Multihash) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.locations, locKey{space, string(digest)})
	return nil
}

// EncryptionParamsStore ======================================================

func (m *MemStore) PutEncryptionParams(_ context.Context, params registry.BlobEncryptionParams) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.encParams[locKey{params.Space, string(params.Digest)}] = cloneEncryptionParams(params)
	return nil
}

func (m *MemStore) GetEncryptionParams(_ context.Context, space did.DID, digest multihash.Multihash) (*registry.BlobEncryptionParams, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	params, ok := m.encParams[locKey{space, string(digest)}]
	if !ok {
		return nil, registry.ErrNotFound
	}
	cp := cloneEncryptionParams(params)
	return &cp, nil
}

func (m *MemStore) GetEncryptionParamsBatch(_ context.Context, space did.DID, digests []multihash.Multihash) (map[string]registry.BlobEncryptionParams, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]registry.BlobEncryptionParams, len(digests))
	for _, d := range digests {
		if params, ok := m.encParams[locKey{space, string(d)}]; ok {
			out[string(d)] = cloneEncryptionParams(params)
		}
	}
	return out, nil
}

func (m *MemStore) DeleteEncryptionParams(_ context.Context, space did.DID, digest multihash.Multihash) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.encParams, locKey{space, string(digest)})
	return nil
}

func (m *MemStore) RewrapEncryptionParams(_ context.Context, space did.DID, digest multihash.Multihash, wrappedCEK []byte, keyVersion string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := locKey{space, string(digest)}
	params, ok := m.encParams[key]
	if !ok {
		return registry.ErrNotFound
	}
	params.RegionWrappedCEK = bytes.Clone(wrappedCEK)
	params.RegionKeyVersion = keyVersion
	m.encParams[key] = params
	return nil
}

// ParkStore ==================================================================

func (m *MemStore) PutPark(_ context.Context, p registry.BlobPark) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.parks[locKey{p.Space, string(p.Digest)}]; ok {
		return false, nil
	}
	cp := p
	cp.Digest = bytes.Clone(p.Digest)
	cp.AddTask = bytes.Clone(p.AddTask)
	cp.AcceptTask = bytes.Clone(p.AcceptTask)
	cp.PutInvocation = bytes.Clone(p.PutInvocation)
	m.parks[locKey{p.Space, string(p.Digest)}] = cp
	return true, nil
}

func (m *MemStore) GetPark(_ context.Context, space did.DID, digest multihash.Multihash) (*registry.BlobPark, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	park, ok := m.parks[locKey{space, string(digest)}]
	if !ok {
		return nil, registry.ErrNotFound
	}
	cp := park
	cp.Digest = bytes.Clone(park.Digest)
	cp.AddTask = bytes.Clone(park.AddTask)
	cp.AcceptTask = bytes.Clone(park.AcceptTask)
	cp.PutInvocation = bytes.Clone(park.PutInvocation)
	return &cp, nil
}

func (m *MemStore) HasParkFor(_ context.Context, addTask []byte) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, park := range m.parks {
		if bytes.Equal(park.AddTask, addTask) {
			return true, nil
		}
	}
	return false, nil
}

func (m *MemStore) DeletePark(_ context.Context, space did.DID, digest multihash.Multihash) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.parks, locKey{space, string(digest)})
	return nil
}

// ConcludePark swaps the park for the location under one lock, as Postgres
// does in one transaction, when the park is the one addTask names.
func (m *MemStore) ConcludePark(_ context.Context, loc registry.BlobLocation, addTask []byte) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := locKey{loc.Space, string(loc.Digest)}
	if park, ok := m.parks[key]; !ok || !bytes.Equal(park.AddTask, addTask) {
		return false, nil
	}
	delete(m.parks, key)
	m.locations[key] = cloneLocation(loc)
	return true, nil
}

// InclusionStore =============================================================

func (m *MemStore) PutInclusions(_ context.Context, incs []registry.BlobInclusion) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, inc := range incs {
		cp := inc
		cp.Digest = bytes.Clone(inc.Digest)
		cp.ShardDigest = bytes.Clone(inc.ShardDigest)
		m.inclusions[locKey{inc.Space, string(inc.Digest)}] = cp
	}
	return nil
}

func (m *MemStore) GetInclusion(_ context.Context, space did.DID, digest multihash.Multihash) (*registry.BlobInclusion, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	inc, ok := m.inclusions[locKey{space, string(digest)}]
	if !ok {
		return nil, registry.ErrNotFound
	}
	cp := inc
	cp.Digest = bytes.Clone(inc.Digest)
	cp.ShardDigest = bytes.Clone(inc.ShardDigest)
	return &cp, nil
}

// MultipartStore =============================================================

func (m *MemStore) CreateSession(_ context.Context, s registry.MultipartSession) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.sessions[s.UploadID]; ok {
		return registry.ErrExists
	}
	if s.State == "" {
		s.State = registry.SessionOpen
	}
	if s.CreatedAt.IsZero() {
		s.CreatedAt = time.Now()
	}
	if s.StateChangedAt.IsZero() {
		s.StateChangedAt = s.CreatedAt
	}
	m.sessions[s.UploadID] = cloneSession(s)
	return nil
}

func (m *MemStore) GetSession(_ context.Context, uploadID string) (*registry.MultipartSession, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[uploadID]
	if !ok {
		return nil, registry.ErrNotFound
	}
	cp := cloneSession(s)
	return &cp, nil
}

// LatchSession is the single-winner transition (§7.3): it succeeds only if
// the session is still in `from`, so exactly one concurrent Complete/Abort
// can move it.
func (m *MemStore) LatchSession(_ context.Context, uploadID, from, to string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[uploadID]
	if !ok || s.State != from {
		return false, nil
	}
	s.State = to
	s.StateChangedAt = time.Now()
	m.sessions[uploadID] = s
	return true, nil
}

func (m *MemStore) CompleteSession(_ context.Context, uploadID, etag, versionID string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[uploadID]
	if !ok || s.State != registry.SessionCompleting {
		return false, nil
	}
	s.State = registry.SessionCompleted
	s.CommittedETag = etag
	s.CommittedVersionID = versionID
	s.StateChangedAt = time.Now()
	m.sessions[uploadID] = s
	return true, nil
}

func (m *MemStore) DeleteSession(_ context.Context, uploadID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.sessions, uploadID)
	delete(m.parts, uploadID)     // FK ON DELETE CASCADE
	delete(m.partTrees, uploadID) // ...and from the parts to their trees
	return nil
}

func (m *MemStore) PutPart(_ context.Context, p registry.MultipartPart, tree *registry.PartTree) ([]registry.PendingRelease, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	sess, ok := m.sessions[p.UploadID]
	if !ok || sess.State != registry.SessionOpen {
		return nil, registry.ErrNotFound // FK to multipart_sessions, and open only
	}
	if p.State == "" {
		p.State = registry.PartSpooled
	}
	if p.CreatedAt.IsZero() {
		p.CreatedAt = time.Now()
	}
	byNum := m.parts[p.UploadID]
	if byNum == nil {
		byNum = map[int]registry.MultipartPart{}
		m.parts[p.UploadID] = byNum
	}
	// The mutex serialises writes of one part number, so the row replaced
	// is the one whose blobs' releases are recorded, as under the advisory
	// lock in Postgres: one record per digest the replaced row named and
	// this part does not, due now, with the part.
	var released []registry.PendingRelease
	if old, ok := byNum[p.PartNumber]; ok {
		keep := map[string]bool{}
		for _, d := range p.BlobDigests {
			keep[string(d)] = true
		}
		for _, d := range old.BlobDigests {
			if keep[string(d)] {
				continue
			}
			keep[string(d)] = true
			released = append(released, m.enqueueReleaseLocked(sess.Space, d, time.Now()))
		}
	}
	byNum[p.PartNumber] = clonePart(p)
	// The tree row follows the part row, as in Postgres.
	trees := m.partTrees[p.UploadID]
	if tree == nil {
		delete(trees, p.PartNumber)
		return released, nil
	}
	if trees == nil {
		trees = map[int]*registry.PartTree{}
		m.partTrees[p.UploadID] = trees
	}
	trees[p.PartNumber] = cloneTree(tree)
	return released, nil
}

// MoveParts mirrors Postgres's guarded update: each row must still be in
// from and still name the digests it was read with.
func (m *MemStore) MoveParts(_ context.Context, parts []registry.MultipartPart, from, to string) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	moved := 0
	for _, want := range parts {
		p, ok := m.parts[want.UploadID][want.PartNumber]
		sameDigests := slices.EqualFunc(p.BlobDigests, want.BlobDigests, func(a, b multihash.Multihash) bool { return bytes.Equal(a, b) })
		if !ok || p.State != from || !sameDigests {
			continue
		}
		p.State = to
		m.parts[want.UploadID][want.PartNumber] = p
		moved++
	}
	return moved, nil
}

// ListParkedParts returns copies of up to limit of uploadID's parked parts,
// in part-number order.
func (m *MemStore) ListParkedParts(ctx context.Context, uploadID string, limit int) ([]registry.MultipartPart, error) {
	parts, err := m.ListParts(ctx, uploadID)
	if err != nil {
		return nil, err
	}
	var out []registry.MultipartPart
	for _, p := range parts {
		if p.State == registry.PartParked && len(out) < limit {
			out = append(out, p)
		}
	}
	return out, nil
}

// OldestParkedPart returns when the oldest parked part of an open or
// completing session was recorded.
func (m *MemStore) OldestParkedPart(context.Context) (time.Time, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var oldest time.Time
	found := false
	for uploadID, byNum := range m.parts {
		s, ok := m.sessions[uploadID]
		if !ok || (s.State != registry.SessionOpen && s.State != registry.SessionCompleting) {
			continue
		}
		for _, p := range byNum {
			if p.State == registry.PartParked && (!found || p.CreatedAt.Before(oldest)) {
				oldest, found = p.CreatedAt, true
			}
		}
	}
	return oldest, found, nil
}

// ListUploadsWithParkedParts returns the open and completing sessions with a
// parked part.
func (m *MemStore) ListUploadsWithParkedParts(context.Context) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []string
	for uploadID, byNum := range m.parts {
		s, ok := m.sessions[uploadID]
		if !ok || (s.State != registry.SessionOpen && s.State != registry.SessionCompleting) {
			continue
		}
		for _, p := range byNum {
			if p.State == registry.PartParked {
				out = append(out, uploadID)
				break
			}
		}
	}
	sort.Strings(out)
	return out, nil
}

// ListPartTrees returns copies of the upload's tree rows by part number.
func (m *MemStore) ListPartTrees(_ context.Context, uploadID string) (map[int]*registry.PartTree, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := map[int]*registry.PartTree{}
	for n, t := range m.partTrees[uploadID] {
		out[n] = cloneTree(t)
	}
	return out, nil
}

// DeletePartTrees mirrors the tree table's delete: the part records stay,
// their trees go.
func (m *MemStore) DeletePartTrees(_ context.Context, uploadID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.partTrees, uploadID)
	return nil
}

func (m *MemStore) ListParts(_ context.Context, uploadID string) ([]registry.MultipartPart, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	byNum := m.parts[uploadID]
	if len(byNum) == 0 {
		return nil, nil
	}
	nums := make([]int, 0, len(byNum))
	for n := range byNum {
		nums = append(nums, n)
	}
	// Insertion sort: part counts are tiny.
	for i := 1; i < len(nums); i++ {
		for j := i; j > 0 && nums[j-1] > nums[j]; j-- {
			nums[j-1], nums[j] = nums[j], nums[j-1]
		}
	}
	out := make([]registry.MultipartPart, 0, len(nums))
	for _, n := range nums {
		out = append(out, clonePart(byNum[n]))
	}
	return out, nil
}

func (m *MemStore) ListSessions(_ context.Context, bucket string) ([]registry.MultipartSession, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []registry.MultipartSession
	for _, s := range m.sessions {
		if s.Bucket == bucket {
			out = append(out, cloneSession(s))
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ObjectKey != out[j].ObjectKey {
			return out[i].ObjectKey < out[j].ObjectKey
		}
		if !out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].CreatedAt.Before(out[j].CreatedAt)
		}
		return out[i].UploadID < out[j].UploadID
	})
	return out, nil
}

func (m *MemStore) ListStaleSessions(_ context.Context, state string, cutoff time.Time) ([]registry.MultipartSession, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []registry.MultipartSession
	for _, s := range m.sessions {
		if s.State == state && s.StateChangedAt.Before(cutoff) {
			out = append(out, cloneSession(s))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].StateChangedAt.Before(out[j].StateChangedAt) })
	return out, nil
}

func (m *MemStore) CountLivePartRefs(_ context.Context, space did.DID, digest multihash.Multihash) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.countLivePartRefsLocked(&space, digest), nil
}

// countLivePartRefsLocked counts the parts of in-flight sessions that name
// digest: space's sessions, or every space's when space is nil.
func (m *MemStore) countLivePartRefsLocked(space *did.DID, digest multihash.Multihash) int {
	n := 0
	for uploadID, byNum := range m.parts {
		s, ok := m.sessions[uploadID]
		if !ok || (s.State != registry.SessionOpen && s.State != registry.SessionCompleting) {
			continue
		}
		if space != nil && s.Space != *space {
			continue
		}
		for _, p := range byNum {
			for _, d := range p.BlobDigests {
				if bytes.Equal(d, digest) {
					n++
					break
				}
			}
		}
	}
	return n
}

func (m *MemStore) CountPartRefs(_ context.Context, digest multihash.Multihash, excludeUploadID string) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for uploadID, byNum := range m.parts {
		if uploadID == excludeUploadID {
			continue
		}
		for _, p := range byNum {
			for _, d := range p.BlobDigests {
				if bytes.Equal(d, digest) {
					n++
					break
				}
			}
		}
	}
	return n, nil
}

// GCStore ====================================================================

func (m *MemStore) AddGCCandidate(_ context.Context, cidBytes []byte, _ string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.gcCands[string(cidBytes)] = struct{}{}
	return nil
}

// RevocationCursorStore ======================================================

func (m *MemStore) GetRevocationCursor(_ context.Context) (*registry.RevocationCursor, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.revCursor == nil {
		return nil, registry.ErrNotFound
	}
	cur := *m.revCursor
	return &cur, nil
}

func (m *MemStore) PutRevocationCursor(_ context.Context, cur registry.RevocationCursor) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.revCursor = &cur
	return nil
}

// clone helpers ==============================================================

func cloneSession(s registry.MultipartSession) registry.MultipartSession {
	if s.Metadata != nil {
		md := make(map[string]string, len(s.Metadata))
		for k, v := range s.Metadata {
			md[k] = v
		}
		s.Metadata = md
	}
	return s
}

// cloneLocation deep-copies a BlobLocation's digest so the stored copy and any
// returned copy never alias the caller's slice.
func cloneLocation(loc registry.BlobLocation) registry.BlobLocation {
	loc.Digest = bytes.Clone(loc.Digest)
	return loc
}

// cloneEncryptionParams deep-copies a BlobEncryptionParams' byte-slice fields
// so the stored copy and any returned copy never alias the caller's slices.
func cloneEncryptionParams(p registry.BlobEncryptionParams) registry.BlobEncryptionParams {
	p.Digest = bytes.Clone(p.Digest)
	p.RegionWrappedCEK = bytes.Clone(p.RegionWrappedCEK)
	p.AAD = bytes.Clone(p.AAD)
	return p
}

func clonePart(p registry.MultipartPart) registry.MultipartPart {
	p.ETagMD5 = bytes.Clone(p.ETagMD5)
	if p.BlobDigests != nil {
		ds := make([]multihash.Multihash, len(p.BlobDigests))
		for i, d := range p.BlobDigests {
			ds[i] = bytes.Clone(d)
		}
		p.BlobDigests = ds
	}
	p.SpanLengths = slices.Clone(p.SpanLengths)
	return p
}

func cloneTree(t *registry.PartTree) *registry.PartTree {
	c := *t
	c.Nodes = bytes.Clone(t.Nodes)
	c.Blocks = bytes.Clone(t.Blocks)
	c.Head = bytes.Clone(t.Head)
	c.Tail = bytes.Clone(t.Tail)
	c.Root = bytes.Clone(t.Root)
	return &c
}

// StreamStore ================================================================

func (m *MemStore) PutStream(_ context.Context, s registry.BlobStream) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := s
	cp.AddTask = bytes.Clone(s.AddTask)
	if cp.CreatedAt.IsZero() {
		cp.CreatedAt = time.Now()
	}
	if cp.TouchedAt.IsZero() {
		cp.TouchedAt = cp.CreatedAt
	}
	m.streams[string(s.AddTask)] = cp
	return nil
}

func (m *MemStore) TouchStreams(_ context.Context, addTasks [][]byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	for _, addTask := range addTasks {
		if s, ok := m.streams[string(addTask)]; ok {
			s.TouchedAt = now
			m.streams[string(addTask)] = s
		}
	}
	return nil
}

func (m *MemStore) DeleteStream(_ context.Context, addTask []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.streams, string(addTask))
	return nil
}

func (m *MemStore) ListStaleStreams(_ context.Context, olderThan time.Time, limit int) ([]registry.BlobStream, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []registry.BlobStream
	for _, s := range m.streams {
		if s.TouchedAt.Before(olderThan) {
			cp := s
			cp.AddTask = bytes.Clone(s.AddTask)
			out = append(out, cp)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].TouchedAt.Before(out[j].TouchedAt) })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// UploadRegistrationStore =====================================================

func (m *MemStore) ListDueUploadRegistrations(_ context.Context, now time.Time, limit int) ([]registry.UploadRegistration, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Per key, only the unbroken run of due rows from its oldest: once a key
	// has a row waiting out a backoff, its later rows wait too, or a
	// retraction could be replayed ahead of the addition it retires.
	blocked := map[string]bool{}
	var out []registry.UploadRegistration
	for _, reg := range m.uploadRegs {
		// A dead-lettered row is out of the sweep and does not hold its key back.
		if reg.DeadLetteredAt != nil {
			continue
		}
		key := reg.Bucket + "\x00" + reg.ObjectKey
		if blocked[key] || reg.NextAt.After(now) {
			blocked[key] = true
			continue
		}
		out = append(out, reg)
		if limit > 0 && len(out) == limit {
			break
		}
	}
	return out, nil
}

func (m *MemStore) ListUploadRegistrationsBySpace(_ context.Context, space did.DID) ([]registry.UploadRegistration, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []registry.UploadRegistration
	for _, reg := range m.uploadRegs {
		if reg.Space == space {
			out = append(out, reg)
		}
	}
	return out, nil
}

func (m *MemStore) RefreshUploadRegistrationProofs(_ context.Context, seq int64, proofs []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, reg := range m.uploadRegs {
		if reg.Seq == seq {
			m.uploadRegs[i].Proofs = proofs
			return nil
		}
	}
	return nil
}

func (m *MemStore) DeadLetterUploadRegistrations(_ context.Context, seqs []int64, reason string) error {
	if len(seqs) == 0 {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	park := make(map[int64]bool, len(seqs))
	for _, seq := range seqs {
		park[seq] = true
	}
	now := time.Now()
	for i, reg := range m.uploadRegs {
		if park[reg.Seq] && reg.DeadLetteredAt == nil {
			m.uploadRegs[i].DeadLetteredAt = &now
			m.uploadRegs[i].DeadLetterReason = reason
		}
	}
	return nil
}

func (m *MemStore) ListDeadLetteredUploadRegistrations(_ context.Context, limit int) ([]registry.UploadRegistration, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []registry.UploadRegistration
	for _, reg := range m.uploadRegs {
		if reg.DeadLetteredAt == nil {
			continue
		}
		out = append(out, reg)
		if limit > 0 && len(out) == limit {
			break
		}
	}
	return out, nil
}

func (m *MemStore) DeleteUploadRegistrations(_ context.Context, seqs []int64) error {
	if len(seqs) == 0 {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	drop := make(map[int64]bool, len(seqs))
	for _, seq := range seqs {
		drop[seq] = true
	}
	kept := m.uploadRegs[:0]
	for _, reg := range m.uploadRegs {
		if !drop[reg.Seq] {
			kept = append(kept, reg)
		}
	}
	m.uploadRegs = kept
	return nil
}

func (m *MemStore) RescheduleUploadRegistrations(_ context.Context, seqs []int64, nextAt time.Time) error {
	if len(seqs) == 0 {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	hold := make(map[int64]bool, len(seqs))
	for _, seq := range seqs {
		hold[seq] = true
	}
	for i, reg := range m.uploadRegs {
		if hold[reg.Seq] {
			m.uploadRegs[i].Attempts++
			m.uploadRegs[i].NextAt = nextAt
		}
	}
	return nil
}

// IntentByDigest returns the one intent for digest, in whichever space holds
// it, or ErrNotFound. Test-only: an intent is keyed by (space, digest), and a
// test that wrote the bytes to one bucket asks after them by digest alone.
// More than one intent is an error, since the digest alone does not pick
// one.
func (m *MemStore) IntentByDigest(_ context.Context, digest multihash.Multihash) (*registry.UploadIntent, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var found *registry.UploadIntent
	for k, in := range m.intents {
		if k.digest != string(digest) {
			continue
		}
		if found != nil {
			return nil, fmt.Errorf("inmem: digest %x has intents in %s and %s", digest, found.Space, in.Space)
		}
		cp := in
		cp.Digest = bytes.Clone(in.Digest)
		found = &cp
	}
	if found == nil {
		return nil, registry.ErrNotFound
	}
	return found, nil
}

// ParkByDigest is IntentByDigest for parks.
func (m *MemStore) ParkByDigest(_ context.Context, digest multihash.Multihash) (*registry.BlobPark, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var found *registry.BlobPark
	for k, park := range m.parks {
		if k.digest != string(digest) {
			continue
		}
		if found != nil {
			return nil, fmt.Errorf("inmem: digest %x has parks in %s and %s", digest, found.Space, park.Space)
		}
		cp := park
		cp.Digest = bytes.Clone(park.Digest)
		cp.AddTask = bytes.Clone(park.AddTask)
		cp.AcceptTask = bytes.Clone(park.AcceptTask)
		cp.PutInvocation = bytes.Clone(park.PutInvocation)
		found = &cp
	}
	if found == nil {
		return nil, registry.ErrNotFound
	}
	return found, nil
}

func (m *MemStore) StartRelease(_ context.Context, space did.DID, digest multihash.Multihash) (registry.ReleaseStart, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := locKey{space, string(digest)}
	pr, ok := m.releases[k]
	if !ok {
		return registry.ReleaseCancelled, nil
	}
	if !pr.StartedAt.IsZero() {
		return registry.ReleaseStarted, nil // a retry: the release owns the blob since it started
	}
	if m.countLivePartRefsLocked(&space, digest) > 0 {
		return registry.ReleaseReferenced, nil
	}
	if m.countClaimsLocked(space, digest) > 0 {
		delete(m.releases, k) // stale: gone here, so no caller deletes a replacement
		return registry.ReleaseClaimed, nil
	}
	if pr.StartedAt.IsZero() {
		pr.StartedAt = time.Now()
		m.releases[k] = pr
	}
	return registry.ReleaseStarted, nil
}

func (m *MemStore) GetRelease(_ context.Context, space did.DID, digest multihash.Multihash) (*registry.PendingRelease, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	pr, ok := m.releases[locKey{space, string(digest)}]
	if !ok {
		return nil, registry.ErrNotFound
	}
	cp := pr
	cp.Digest = multihash.Multihash(bytes.Clone(pr.Digest))
	return &cp, nil
}

func (m *MemStore) ReleasingDigests(_ context.Context, space did.DID, digests []multihash.Multihash) (map[string]bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := map[string]bool{}
	for _, d := range digests {
		if _, ok := m.releases[locKey{space, string(d)}]; ok {
			out[string(d)] = true
		}
	}
	return out, nil
}

func (m *MemStore) CancelRelease(_ context.Context, space did.DID, digest multihash.Multihash) (registry.ReleaseCancel, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := locKey{space, string(digest)}
	pr, ok := m.releases[k]
	if !ok {
		return registry.ReleaseNone, nil
	}
	if !pr.StartedAt.IsZero() {
		return registry.ReleaseInProgress, nil
	}
	delete(m.releases, k)
	return registry.ReleaseCancelledByWrite, nil
}
