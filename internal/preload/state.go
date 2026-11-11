package preload

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// stateVersion is the version of the state file's format.
const stateVersion = 1

// State is what a load records of its progress, so that a later load
// resumes where it stopped and the validation knows the real objects' keys.
type State struct {
	Version int `json:"version"`
	// Seed and KeyPrefix are the load's; a load with others would write
	// different keys, so it refuses the state.
	Seed      string                  `json:"seed"`
	KeyPrefix string                  `json:"key_prefix"`
	Buckets   map[string]*BucketState `json:"buckets"`
}

// BucketState is one bucket's progress.
type BucketState struct {
	// Ready is set once the bucket exists and its encryption is set.
	Ready bool `json:"ready"`
	// Encryption is the default encryption set on the bucket, "" when the
	// gateway's was left. A later load asking for another is refused.
	Encryption string `json:"encryption,omitempty"`
	// Bulk is the number of bulk objects written: every bulk object below
	// it was. A load stopped part way may have written some at or above it.
	Bulk uint64 `json:"bulk"`
	// Real lists the real objects written, in index order: Real[j] is
	// real object j.
	Real []RealObject `json:"real,omitempty"`
}

// RealObject is a real object a load wrote.
type RealObject struct {
	Index uint64 `json:"index"`
	Key   string `json:"key"`
	Size  int64  `json:"size"`
}

// ReadState reads the state file at path. A missing file is a new state for
// seed and keyPrefix; an existing one must have been written with the same
// seed and key prefix.
func ReadState(path, seed, keyPrefix string) (*State, error) {
	b, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return &State{
			Version:   stateVersion,
			Seed:      seed,
			KeyPrefix: keyPrefix,
			Buckets:   make(map[string]*BucketState),
		}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read state: %w", err)
	}
	var s State
	if err := json.Unmarshal(b, &s); err != nil {
		return nil, fmt.Errorf("read state %s: %w", path, err)
	}
	if s.Version != stateVersion {
		return nil, fmt.Errorf("state %s has version %d, want %d", path, s.Version, stateVersion)
	}
	if s.Seed != seed || s.KeyPrefix != keyPrefix {
		return nil, fmt.Errorf("state %s was written with seed %q and key prefix %q, not %q and %q",
			path, s.Seed, s.KeyPrefix, seed, keyPrefix)
	}
	if s.Buckets == nil {
		s.Buckets = make(map[string]*BucketState)
	}
	return &s, nil
}

// Bucket returns bucket's progress, adding it when the state has none.
func (s *State) Bucket(name string) *BucketState {
	b, ok := s.Buckets[name]
	if !ok {
		b = &BucketState{}
		s.Buckets[name] = b
	}
	return b
}

// writeState writes s to path through a temporary file, so that a crash
// leaves either the old state or the new one.
func writeState(path string, s *State) error {
	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".tmp*")
	if err != nil {
		return fmt.Errorf("write state: %w", err)
	}
	tmp := f.Name()
	defer os.Remove(tmp) // a no-op after the rename
	if _, err := f.Write(append(b, '\n')); err != nil {
		f.Close()
		return fmt.Errorf("write state: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("write state: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("write state: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("write state: %w", err)
	}
	return nil
}
