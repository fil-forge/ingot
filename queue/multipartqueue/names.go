package multipartqueue

// Name is queue-multipart-upload, the task queue a multipart upload's
// changes run on: its creation, its parts, the scheduling of their accepts,
// its Complete and its Abort. Its subject is the upload ID, so one change to
// an upload runs at a time. It is first in the queue order: a create runs
// queue-bucket.admit-upload as a child, and a Complete runs
// queue-bucket.commit as one.
const Name = "queue-multipart-upload"

// The queue-multipart-upload actions.
const (
	CreateAction   = "create"
	AddPartAction  = "add-part"
	AcceptAction   = "accept"
	CompleteAction = "complete"
	AbortAction    = "abort"
	ReapAction     = "reap"
)
