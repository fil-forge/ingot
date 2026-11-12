package bucketqueue

import "github.com/fil-forge/ucantone/did"

// Name is queue-bucket, the task queue a bucket's changes run on: its object
// commits, the shipping of its catalog, its settings and its lifecycle. Its
// subject is the bucket's space (Subject), so one change to a bucket runs at
// a time.
const Name = "queue-bucket"

// The queue-bucket actions.
const (
	CreateAction        = "create"
	CommitAction        = "commit"
	ShipAction          = "ship"
	SetVersioningAction = "set-versioning"
	SetLockConfigAction = "set-object-lock-config"
	SetTaggingAction    = "set-tagging"
	SetCORSAction       = "set-cors"
	SetEncryptionAction = "set-encryption"
	CloseAction         = "close"
	AdmitUploadAction   = "admit-upload"
	DeleteAction        = "delete"
)

// Subject is the subject of space's bucket on queue-bucket.
func Subject(space did.DID) string { return space.String() }
