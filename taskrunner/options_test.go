package taskrunner_test

import (
	"testing"
	"time"

	"github.com/fil-forge/ingot/taskrunner"
)

func TestANegativeRetentionIsRefused(t *testing.T) {
	for name, qo := range map[string]taskrunner.QueueOptions{
		"retention":        {Name: "q", Retention: -time.Hour},
		"failed retention": {Name: "q", FailedRetention: -time.Hour},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := taskrunner.New(nil, taskrunner.Options{Schema: "s", Queues: []taskrunner.QueueOptions{qo}}); err == nil {
				t.Error("New accepted a negative retention")
			}
		})
	}
}
