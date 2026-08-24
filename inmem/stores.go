package inmem

import (
	"bytes"
	"context"
	"sort"
	"time"

	"github.com/fil-forge/ingot/registry"
	"github.com/fil-forge/ucantone/did"
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

func (m *MemStore) CountClaims(_ context.Context, space did.DID, digest []byte) (int, error) {
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
	m.mu.Lock()
	defer m.mu.Unlock()
	m.locations[locKey{loc.Space, string(loc.Digest)}] = cloneLocation(loc)
	return nil
}

func (m *MemStore) GetLocation(_ context.Context, space did.DID, digest []byte) (*registry.BlobLocation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	loc, ok := m.locations[locKey{space, string(digest)}]
	if !ok {
		return nil, registry.ErrNotFound
	}
	cp := cloneLocation(loc)
	return &cp, nil
}

func (m *MemStore) DeleteLocation(_ context.Context, space did.DID, digest []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.locations, locKey{space, string(digest)})
	return nil
}

// EncryptionParamsStore ======================================================

func (m *MemStore) PutEncryptionParams(_ context.Context, params registry.BlobEncryptionParams) error {
	// Match the Postgres store, whose columns are all NOT NULL.
	if err := params.Validate(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.encParams[locKey{params.Space, string(params.Digest)}] = cloneEncryptionParams(params)
	return nil
}

func (m *MemStore) GetEncryptionParams(_ context.Context, space did.DID, digest []byte) (*registry.BlobEncryptionParams, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	params, ok := m.encParams[locKey{space, string(digest)}]
	if !ok {
		return nil, registry.ErrNotFound
	}
	cp := cloneEncryptionParams(params)
	return &cp, nil
}

func (m *MemStore) DeleteEncryptionParams(_ context.Context, space did.DID, digest []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.encParams, locKey{space, string(digest)})
	return nil
}

// ParkStore ==================================================================

func (m *MemStore) PutPark(_ context.Context, p registry.BlobPark) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := p
	cp.Digest = cloneBytes(p.Digest)
	cp.AddTask = cloneBytes(p.AddTask)
	cp.AcceptTask = cloneBytes(p.AcceptTask)
	cp.PutInvocation = cloneBytes(p.PutInvocation)
	m.parks[string(p.Digest)] = cp
	return nil
}

func (m *MemStore) GetPark(_ context.Context, digest []byte) (*registry.BlobPark, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	park, ok := m.parks[string(digest)]
	if !ok {
		return nil, registry.ErrNotFound
	}
	cp := park
	cp.Digest = cloneBytes(park.Digest)
	cp.AddTask = cloneBytes(park.AddTask)
	cp.AcceptTask = cloneBytes(park.AcceptTask)
	cp.PutInvocation = cloneBytes(park.PutInvocation)
	return &cp, nil
}

func (m *MemStore) DeletePark(_ context.Context, digest []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.parks, string(digest))
	return nil
}

// InclusionStore =============================================================

func (m *MemStore) PutInclusions(_ context.Context, incs []registry.BlobInclusion) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, inc := range incs {
		cp := inc
		cp.Digest = cloneBytes(inc.Digest)
		cp.ShardDigest = cloneBytes(inc.ShardDigest)
		m.inclusions[locKey{inc.Space, string(inc.Digest)}] = cp
	}
	return nil
}

func (m *MemStore) GetInclusion(_ context.Context, space did.DID, digest []byte) (*registry.BlobInclusion, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	inc, ok := m.inclusions[locKey{space, string(digest)}]
	if !ok {
		return nil, registry.ErrNotFound
	}
	cp := inc
	cp.Digest = cloneBytes(inc.Digest)
	cp.ShardDigest = cloneBytes(inc.ShardDigest)
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
	if p.CreatedAt.IsZero() {
		p.CreatedAt = time.Now()
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
		if s.State == state && s.CreatedAt.Before(cutoff) {
			out = append(out, cloneSession(s))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out, nil
}

func (m *MemStore) CountPartRefs(_ context.Context, digest []byte, excludeUploadID string) (int, error) {
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
	loc.Digest = cloneBytes(loc.Digest)
	return loc
}

// cloneEncryptionParams deep-copies a BlobEncryptionParams' byte-slice fields
// so the stored copy and any returned copy never alias the caller's slices.
func cloneEncryptionParams(p registry.BlobEncryptionParams) registry.BlobEncryptionParams {
	p.Digest = cloneBytes(p.Digest)
	p.BaseNonce = cloneBytes(p.BaseNonce)
	p.AAD = cloneBytes(p.AAD)
	return p
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
