package s3frontend

import (
	"testing"

	"github.com/fil-forge/versitygw/s3err"
	"github.com/stretchr/testify/require"

	msbucket "github.com/fil-forge/ingot/bucket"
)

// TestPartRange locks the GET/HEAD ?partNumber=N contract (docs/architecture.md
// §7.2): a multipart object addresses parts by recorded boundary, a single-PUT
// object exposes the whole body as part 1, a zero-byte object yields no range,
// and a partNumber past the part count is rejected with 416 InvalidPartNumber,
// for both multipart and non-multipart objects (matching AWS).
func TestPartRange(t *testing.T) {
	const mib = int64(5 << 20)
	// A 3-part multipart object: 5 MiB + 5 MiB + 5 MiB = 15 MiB.
	mp := msbucket.Body{Size: 3 * mib, PartSizes: []int64{mib, mib, mib}}
	// A single-PUT (non-multipart) object: no PartSizes.
	single := msbucket.Body{Size: 1234}
	empty := msbucket.Body{Size: 0}
	// A multipart object whose 2nd part is empty: 5 MiB + 0 + 5 MiB.
	zeroPart := msbucket.Body{Size: 2 * mib, PartSizes: []int64{mib, 0, mib}}

	three := int32(3)
	cases := []struct {
		name       string
		body       msbucket.Body
		part       int32
		wantStart  int64
		wantLength int64
		wantRange  bool
		wantCount  *int32
		wantErr    string // s3err Code; "" = no error
	}{
		{"mp part 1", mp, 1, 0, mib, true, &three, ""},
		{"mp part 2", mp, 2, mib, mib, true, &three, ""},
		{"mp part 3", mp, 3, 2 * mib, mib, true, &three, ""},
		{"mp exceeds", mp, 4, 0, 0, false, nil, "InvalidPartNumber"},
		// A zero-length part: parts-count still reported, but no byte range (so no
		// Content-Range), mirroring a zero-byte object rather than emitting a
		// malformed "bytes start-(start-1)".
		{"mp zero-length part", zeroPart, 2, mib, 0, false, &three, ""},
		{"single part 1 = whole object", single, 1, 0, 1234, true, nil, ""},
		{"single part 2 exceeds", single, 2, 0, 0, false, nil, "InvalidPartNumber"},
		{"empty part 1 = no range", empty, 1, 0, 0, false, nil, ""},
		{"empty part 2 exceeds", empty, 2, 0, 0, false, nil, "InvalidPartNumber"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			start, length, isRange, count, err := partRange(tc.body, tc.part)
			if tc.wantErr != "" {
				var apiErr s3err.APIError
				require.ErrorAs(t, err, &apiErr, "err = %v, want code %s", err, tc.wantErr)
				require.Equal(t, tc.wantErr, apiErr.Code, "err")
				return
			}
			require.NoError(t, err, "unexpected err")
			require.Equal(t, tc.wantStart, start, "start")
			require.Equal(t, tc.wantLength, length, "length")
			require.Equal(t, tc.wantRange, isRange, "range")
			if tc.wantCount == nil {
				require.Nil(t, count, "PartsCount (non-multipart)")
			} else {
				require.NotNil(t, count, "PartsCount")
				require.Equal(t, *tc.wantCount, *count, "PartsCount")
			}
		})
	}
}

// TestSelectBytes_Dispatch confirms selectBytes routes to partRange when a part
// number is present and to Range parsing otherwise (versitygw guarantees the two
// are never supplied together).
func TestSelectBytes_Dispatch(t *testing.T) {
	body := msbucket.Body{Size: 100, PartSizes: []int64{40, 60}}

	// partNumber path: part 2 spans [40,100).
	pn := int32(2)
	start, length, isRange, count, err := selectBytes(body, &pn, "")
	require.NoError(t, err, "partNumber dispatch: start=%d len=%d range=%v count=%v err=%v", start, length, isRange, count, err)
	require.Equal(t, int64(40), start, "partNumber dispatch: start=%d len=%d range=%v count=%v err=%v", start, length, isRange, count, err)
	require.Equal(t, int64(60), length, "partNumber dispatch: start=%d len=%d range=%v count=%v err=%v", start, length, isRange, count, err)
	require.True(t, isRange, "partNumber dispatch: start=%d len=%d range=%v count=%v err=%v", start, length, isRange, count, err)
	require.NotNil(t, count, "partNumber dispatch: start=%d len=%d range=%v count=%v err=%v", start, length, isRange, count, err)
	require.Equal(t, int32(2), *count, "partNumber dispatch: start=%d len=%d range=%v count=%v err=%v", start, length, isRange, count, err)

	// Range path: no partNumber, a byte range yields a nil parts-count.
	start, length, isRange, count, err = selectBytes(body, nil, "bytes=10-19")
	require.NoError(t, err, "range dispatch: start=%d len=%d range=%v count=%v err=%v", start, length, isRange, count, err)
	require.Equal(t, int64(10), start, "range dispatch: start=%d len=%d range=%v count=%v err=%v", start, length, isRange, count, err)
	require.Equal(t, int64(10), length, "range dispatch: start=%d len=%d range=%v count=%v err=%v", start, length, isRange, count, err)
	require.True(t, isRange, "range dispatch: start=%d len=%d range=%v count=%v err=%v", start, length, isRange, count, err)
	require.Nil(t, count, "range dispatch: start=%d len=%d range=%v count=%v err=%v", start, length, isRange, count, err)

	// No selector: whole object, not a range.
	start, length, isRange, count, err = selectBytes(body, nil, "")
	require.NoError(t, err, "whole-object dispatch: start=%d len=%d range=%v count=%v err=%v", start, length, isRange, count, err)
	require.Zero(t, start, "whole-object dispatch: start=%d len=%d range=%v count=%v err=%v", start, length, isRange, count, err)
	require.Equal(t, int64(100), length, "whole-object dispatch: start=%d len=%d range=%v count=%v err=%v", start, length, isRange, count, err)
	require.False(t, isRange, "whole-object dispatch: start=%d len=%d range=%v count=%v err=%v", start, length, isRange, count, err)
	require.Nil(t, count, "whole-object dispatch: start=%d len=%d range=%v count=%v err=%v", start, length, isRange, count, err)
}
