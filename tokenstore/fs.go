// Carried copy of github.com/fil-forge/guppy/pkg/tokenstore/fs.go.
package tokenstore

import (
	"context"
	"fmt"
	"iter"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sync"

	"github.com/fil-forge/ucantone/did"
	"github.com/fil-forge/ucantone/ucan"
	"github.com/fil-forge/ucantone/ucan/container"
	cid "github.com/ipfs/go-cid"
)

const fsStoreFileName = "tokens.cbor"

type FsStore struct {
	data *MemStore
	path string
	mu   sync.RWMutex
}

var _ Store = (*FsStore)(nil)

// NewFsStore creates a new token store backed by the filesystem.
func NewFsStore(rootdir string) (*FsStore, error) {
	err := os.MkdirAll(rootdir, 0755)
	if err != nil {
		return nil, fmt.Errorf("directory %q not writable: %w", rootdir, err)
	}
	filePath := filepath.Join(rootdir, fsStoreFileName)
	store := FsStore{data: NewMemStore(), path: filePath}
	if _, err := os.Stat(filePath); err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
		err := store.save()
		if err != nil {
			return nil, fmt.Errorf("initializing token store: %w", err)
		}
	} else {
		if err := store.load(); err != nil {
			return nil, fmt.Errorf("loading token store: %w", err)
		}
	}
	return &store, nil
}

func (s *FsStore) load() error {
	f, err := os.Open(s.path)
	if err != nil {
		return fmt.Errorf("opening token store file: %w", err)
	}
	defer f.Close()
	var ct container.Container
	if err := ct.UnmarshalCBOR(f); err != nil {
		return fmt.Errorf("unmarshaling token store data: %w", err)
	}
	_ = s.data.AddInvocations(context.Background(), ct.Invocations()...)
	_ = s.data.AddDelegations(context.Background(), ct.Delegations()...)
	_ = s.data.AddReceipts(context.Background(), ct.Receipts()...)
	return nil
}

func (s *FsStore) save() error {
	ct := container.New(
		container.WithInvocations(slices.Collect(maps.Values(s.data.invs))...),
		container.WithDelegations(slices.Collect(maps.Values(s.data.dlgs))...),
		container.WithReceipts(slices.Collect(maps.Values(s.data.rcpts))...),
	)
	// TODO: write to a temp file and then rename
	f, err := os.Create(s.path)
	if err != nil {
		return fmt.Errorf("creating token store file: %w", err)
	}
	defer f.Close()
	if err := ct.MarshalCBOR(f); err != nil {
		return fmt.Errorf("marshaling token store data: %w", err)
	}
	return nil
}

func (s *FsStore) AddDelegations(ctx context.Context, delegations ...ucan.Delegation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_ = s.data.AddDelegations(ctx, delegations...)
	return s.save()
}

func (s *FsStore) ListDelegations(ctx context.Context, aud did.DID, cmd ucan.Command, sub did.DID) iter.Seq2[ucan.Delegation, error] {
	return func(yield func(ucan.Delegation, error) bool) {
		s.mu.RLock()
		defer s.mu.RUnlock()
		for d, err := range s.data.ListDelegations(ctx, aud, cmd, sub) {
			if err != nil {
				yield(nil, err)
				return
			}
			if !yield(d, nil) {
				return
			}
		}
	}
}

func (s *FsStore) Delegations(ctx context.Context) ([]ucan.Delegation, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.data.Delegations(ctx)
}

func (s *FsStore) AddInvocations(ctx context.Context, invocations ...ucan.Invocation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_ = s.data.AddInvocations(ctx, invocations...)
	return s.save()
}

func (s *FsStore) AddReceipts(ctx context.Context, receipts ...ucan.Receipt) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_ = s.data.AddReceipts(ctx, receipts...)
	return s.save()
}

func (s *FsStore) ProofAttestations(ctx context.Context, proofs []ucan.Delegation, authority did.DID) ([]ucan.Invocation, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.data.ProofAttestations(ctx, proofs, authority)
}

func (s *FsStore) ProofChain(ctx context.Context, aud did.DID, cmd ucan.Command, sub did.DID) ([]ucan.Delegation, []cid.Cid, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.data.ProofChain(ctx, aud, cmd, sub)
}

func (s *FsStore) Reset(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_ = s.data.Reset(ctx)
	return s.save()
}
