package fasthttputil

import (
	"context"
	"time"

	"github.com/valyala/fasthttp"
)

// DetachContext returns a context that answers as ctx does, without referring
// to ctx, when ctx is a fasthttp request. Otherwise it returns ctx as is.
//
// versitygw hands each request's *fasthttp.RequestCtx to the backend as its
// context, and fasthttp resets that value for its next request once the
// handler returns. Anything that reads it after then, such as a database
// driver's goroutine watching it for cancellation or work that outlives the
// request, races with the reset. The returned context holds a copy of the
// request's values, which is all a RequestCtx answers Value with, and the
// server's done channel, so it is still cancelled at shutdown.
func DetachContext(ctx context.Context) context.Context {
	rc, ok := ctx.(*fasthttp.RequestCtx)
	if !ok {
		return ctx
	}
	values := map[any]any{}
	rc.VisitUserValuesAll(func(k, v any) {
		values[k] = v
	})
	return detached{values: values, done: rc.Done()}
}

// detached is a fasthttp request's values and its server's done channel.
type detached struct {
	values map[any]any
	done   <-chan struct{}
}

func (detached) Deadline() (time.Time, bool) { return time.Time{}, false }

func (d detached) Done() <-chan struct{} { return d.done }

func (d detached) Err() error {
	select {
	case <-d.done:
		return context.Canceled
	default:
		return nil
	}
}

func (d detached) Value(key any) any { return d.values[key] }
