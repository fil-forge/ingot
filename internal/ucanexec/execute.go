// Package ucanexec provides a small generic helper for executing a UCAN
// invocation against any ucantone execution.Executor and decoding the typed
// success result out of the returned receipt.
//
// It is a trimmed, dependency-light copy of fil-forge/sprue's
// pkg/lib/ucan_client.Execute: the executor is taken as an interface (so the
// same helper drives both the HTTP client used for allocate/accept/publish and
// the libforge retrieval client used for ranged reads), and the sprue-internal
// structured logging (zapipld) has been dropped.
package ucanexec

import (
	"bytes"
	"context"
	"fmt"
	"reflect"

	edm "github.com/fil-forge/ucantone/errors/datamodel"
	"github.com/fil-forge/ucantone/execution"
	"github.com/fil-forge/ucantone/ucan"
	cbg "github.com/whyrusleeping/cbor-gen"
)

// Execute sends inv via executor and decodes the success branch of the
// resulting receipt into T. The receipt and the response metadata container
// are returned alongside the decoded value; for streaming transports (e.g. the
// retrieval client) the bytes are carried on the metadata container, not in T.
func Execute[T cbg.CBORUnmarshaler](
	ctx context.Context,
	executor execution.Executor,
	inv ucan.Invocation,
	options ...execution.RequestOption,
) (T, ucan.Receipt, ucan.Container, error) {
	var zero T

	resp, err := executor.Execute(execution.NewRequest(ctx, inv, options...))
	if err != nil {
		return zero, nil, nil, fmt.Errorf("executing invocation: %w", err)
	}

	rcpt := resp.Receipt()
	o, x := rcpt.Out().Unpack()
	if rcpt.Out().IsErr() {
		var model edm.ErrorModel
		if err := model.UnmarshalCBOR(bytes.NewReader(x)); err != nil {
			return zero, nil, nil, fmt.Errorf("executing invocation: undecodable failure")
		}
		return zero, nil, nil, fmt.Errorf("executing invocation: %w", model)
	}

	// If T is a pointer type, allocate the underlying value so UnmarshalCBOR has
	// a non-nil pointer to write into.
	var ok T
	if typ := reflect.TypeOf(ok); typ != nil && typ.Kind() == reflect.Ptr {
		ok = reflect.New(typ.Elem()).Interface().(T)
	}
	if err := ok.UnmarshalCBOR(bytes.NewReader(o)); err != nil {
		return zero, nil, nil, fmt.Errorf("unmarshaling invocation response: %w", err)
	}
	return ok, rcpt, resp.Metadata(), nil
}
