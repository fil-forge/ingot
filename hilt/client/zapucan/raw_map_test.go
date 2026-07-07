package zapucan_test

import (
	"bytes"
	"testing"

	"github.com/fil-forge/ingot/hilt/client/zapucan"
	"github.com/fil-forge/libforge/testutil"
	"github.com/fil-forge/ucantone/ipld/datamodel"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

func TestRawMap(t *testing.T) {
	log := zaptest.NewLogger(t)
	t.Run("logs raw map", func(t *testing.T) {
		m := datamodel.Map{
			"foo": "bar",
			"baz": 123,
			"nested": datamodel.Map{
				"hello": "world",
			},
			"array":      []string{"a", "b", "c"},
			"mixedArray": []any{"a", 1, true, datamodel.Map{"key": "value"}},
			"nilly":      nil,
			"cid":        testutil.RandomCID(t),
		}
		var buf bytes.Buffer
		if err := m.MarshalCBOR(&buf); err != nil {
			t.Fatalf("failed to marshal map: %v", err)
		}
		log.With(zap.Object("rawMap", zapucan.RawMap(buf.Bytes()))).Info("logging raw map")
	})

	t.Run("raw map empty bytes", func(t *testing.T) {
		log.With(zap.Object("rawMap", zapucan.RawMap([]byte{}))).Info("empty bytes")
	})

	t.Run("raw map invalid bytes (non CBOR)", func(t *testing.T) {
		log.With(zap.Object("rawMap", zapucan.RawMap([]byte{1, 2, 3}))).Info("invalid bytes")
	})
}
