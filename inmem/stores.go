package inmem

import (
	"context"

	"github.com/fil-forge/ingot/registry"
)

// In-memory implementations of the architecture's relational stores
// (registry.BlobRefStore / IntentStore / LocationStore / MultipartStore /
// GCStore), mirroring the Postgres tables so the in-process suite and
// standalone mode exercise the same write/read/delete code paths.

// Compile-time assertions: *MemStore satisfies every store interface.
var (
	_ registry.BlobRefStore   = (*MemStore)(nil)
	_ registry.IntentStore    = (*MemStore)(nil)
	_ registry.LocationStore  = (*MemStore)(nil)
	_ registry.MultipartStore = (*MemStore)(nil)
	_ registry.GCStore        = (*MemStore)(nil)
)

func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	return append([]byte(nil), b...)
}

// BlobRefStore ===============================================================

func (m *MemStore) AddBlobClaim(_ context.Context, c registry.BlobClaim) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := claimKey{string(c.Digest), c.Bucket, c.ObjectKey, c.VersionID}
	cp := c
	cp.Digest = cloneBytes(c.Digest)
	m.blobRefs[k] = cp
	return nil
}

func (m *MemStore) DeleteBlobClaim(_ context.Context, digest []byte, bucket, objectKey, versionID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.blobRefs, claimKey{string(digest), bucket, objectKey, versionID})
	return nil
}

func (m *MemStore) CountClaims(_ context.Context, space string, digest []byte) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	d := string(digest)
	n := 0
	for k, c := range m.blobRefs {
		if k.digest == d && c.Space == space {
			n++
		}
	}
	return n, nil
}

// IntentStore ================================================================

func (m *MemStore) PutIntent(_ context.Context, in registry.UploadIntent) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := in
	cp.Digest = cloneBytes(in.Digest)
	m.intents[string(in.Digest)] = cp
	return nil
}

func (m *MemStore) SetIntentState(_ context.Context, digest []byte, state string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	in, ok := m.intents[string(digest)]
	if !ok {
		return registry.ErrNotFound
	}
	in.State = state
	m.intents[string(digest)] = in
	return nil
}

func (m *MemStore) GetIntent(_ context.Context, digest []byte) (*registry.UploadIntent, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	in, ok := m.intents[string(digest)]
	if !ok {
		return nil, registry.ErrNotFound
	}
	cp := in
	cp.Digest = cloneBytes(in.Digest)
	return &cp, nil
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
		cp.Digest = cloneBytes(in.Digest)
		out = append(out, cp)
	}
	return out, nil
}

func (m *MemStore) DeleteIntent(_ context.Context, digest []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.intents, string(digest))
	return nil
}

// LocationStore ==============================================================

func (m *MemStore) PutLocation(_ context.Context, loc registry.BlobLocation) error {
	// Match the Postgres store: reject a partial FEE set (see ValidateFEE).
	if err := loc.ValidateFEE(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.locations[locKey{loc.Space, string(loc.Digest)}] = cloneLocation(loc)
	return nil
}

func (m *MemStore) GetLocation(_ context.Context, space string, digest []byte) (*registry.BlobLocation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	loc, ok := m.locations[locKey{space, string(digest)}]
	if !ok {
		return nil, registry.ErrNotFound
	}
	cp := cloneLocation(loc)
	return &cp, nil
}

func (m *MemStore) DeleteLocation(_ context.Context, space string, digest []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.locations, locKey{space, string(digest)})
	return nil
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
	m.sessions[uploadID] = s
	return true, nil
}

func (m *MemStore) DeleteSession(_ context.Context, uploadID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.sessions, uploadID)
	delete(m.parts, uploadID) // FK ON DELETE CASCADE
	return nil
}

func (m *MemStore) PutPart(_ context.Context, p registry.MultipartPart) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.sessions[p.UploadID]; !ok {
		return registry.ErrNotFound // FK to multipart_sessions
	}
	if p.State == "" {
		p.State = registry.PartParked
	}
	byNum := m.parts[p.UploadID]
	if byNum == nil {
		byNum = map[int]registry.MultipartPart{}
		m.parts[p.UploadID] = byNum
	}
	byNum[p.PartNumber] = clonePart(p)
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

// GCStore ====================================================================

func (m *MemStore) AddGCCandidate(_ context.Context, cidBytes []byte, _ string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.gcCands[string(cidBytes)] = struct{}{}
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

// cloneLocation deep-copies a BlobLocation's byte-slice fields (the digest and
// the FEE wrap material) so the stored copy and any returned copy never alias
// the caller's slices. Nil slices stay nil, preserving "unencrypted blob".
func cloneLocation(loc registry.BlobLocation) registry.BlobLocation {
	loc.Digest = cloneBytes(loc.Digest)
	loc.RegionWrappedCEK = cloneBytes(loc.RegionWrappedCEK)
	loc.BaseNonce = cloneBytes(loc.BaseNonce)
	loc.ProtectedHeader = cloneBytes(loc.ProtectedHeader)
	return loc
}

func clonePart(p registry.MultipartPart) registry.MultipartPart {
	p.ETagMD5 = cloneBytes(p.ETagMD5)
	if p.BlobDigests != nil {
		ds := make([][]byte, len(p.BlobDigests))
		for i, d := range p.BlobDigests {
			ds[i] = cloneBytes(d)
		}
		p.BlobDigests = ds
	}
	return p
}
