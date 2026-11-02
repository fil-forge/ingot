package taskrunner

import "testing"

func TestFreeSubscriptionsAreRemovedWhenTheirWaitsEnd(t *testing.T) {
	r := &Runner{freed: map[subjectKey]*freeSub{}}
	a, b := subjectKey{"q", "a"}, subjectKey{"q", "b"}

	_, endA1 := r.subscribeFree(a)
	_, endA2 := r.subscribeFree(a)
	_, endB := r.subscribeFree(b)
	endA1()
	if _, ok := r.freed[a]; !ok {
		t.Fatal("subscription removed while another wait on it remains")
	}
	endA2()
	endB()
	if len(r.freed) != 0 {
		t.Errorf("%d subscriptions left after every wait ended", len(r.freed))
	}
}
