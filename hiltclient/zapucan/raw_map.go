package zapucan

import (
	"bytes"

	"github.com/fil-forge/ucantone/ipld/datamodel"
	"go.uber.org/zap/zapcore"
)

// RawMap is a [zapcore.ObjectMarshaler] that decodes the given bytes as a
// CBOR-encoded IPLD map and logs its keys and values.
type RawMap []byte

func (rm RawMap) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	if len(rm) == 0 {
		return nil
	}
	var m datamodel.Map
	if err := m.UnmarshalCBOR(bytes.NewReader(rm)); err != nil {
		return err
	}
	for k, v := range m {
		err := enc.AddReflected(k, v)
		if err != nil {
			return err
		}
	}
	return nil
}
