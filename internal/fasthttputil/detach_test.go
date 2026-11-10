package fasthttputil

import (
	"context"
	"testing"

	"github.com/valyala/fasthttp"
)

type valueKey struct{}

// TestDetachContextKeepsTheRequestsValues: the detached context answers with
// the request's values after fasthttp resets the request, shares the
// server's done channel, and is not the request itself.
func TestDetachContextKeepsTheRequestsValues(t *testing.T) {
	var rc fasthttp.RequestCtx
	rc.Init(&fasthttp.Request{}, nil, nil)
	rc.SetUserValue("named", "a")
	rc.SetUserValue(valueKey{}, 7)

	ctx := DetachContext(&rc)
	if _, same := ctx.(*fasthttp.RequestCtx); same {
		t.Fatal("DetachContext returned the request itself")
	}
	rc.ResetUserValues()
	if got := ctx.Value("named"); got != "a" {
		t.Errorf(`Value("named") after the reset = %v, want "a"`, got)
	}
	if got := ctx.Value(valueKey{}); got != 7 {
		t.Errorf("Value(valueKey{}) after the reset = %v, want 7", got)
	}
	if ctx.Done() != rc.Done() {
		t.Error("the detached context does not share the server's done channel")
	}
	if err := ctx.Err(); err != nil {
		t.Errorf("Err() = %v, want nil while the server runs", err)
	}
}

// TestDetachContextLeavesOtherContexts: a context that is not a fasthttp
// request is returned as is.
func TestDetachContextLeavesOtherContexts(t *testing.T) {
	ctx := context.WithValue(context.Background(), valueKey{}, 1)
	if DetachContext(ctx) != ctx {
		t.Fatal("DetachContext changed a context that is not a fasthttp request")
	}
}
