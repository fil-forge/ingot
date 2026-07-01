package s3frontend

import (
	"errors"
	"testing"

	"github.com/versity/versitygw/s3err"

	msbucket "github.com/fil-forge/ingot/bucket"
)

// TestPartRange locks the GET/HEAD ?partNumber=N contract (docs/architecture.md
// §7.2): a multipart object addresses parts by recorded boundary, a single-PUT
// object exposes the whole body as part 1, a zero-byte object yields no range,
// and a partNumber past the part count is a 416 (ErrInvalidPartNumberRange).
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
		wantErr    bool
	}{
		{"mp part 1", mp, 1, 0, mib, true, &three, false},
		{"mp part 2", mp, 2, mib, mib, true, &three, false},
		{"mp part 3", mp, 3, 2 * mib, mib, true, &three, false},
		{"mp exceeds", mp, 4, 0, 0, false, nil, true},
		// A zero-length part: parts-count still reported, but no byte range (so no
		// Content-Range), mirroring a zero-byte object rather than emitting a
		// malformed "bytes start-(start-1)".
		{"mp zero-length part", zeroPart, 2, mib, 0, false, &three, false},
		{"single part 1 = whole object", single, 1, 0, 1234, true, nil, false},
		{"single part 2 exceeds", single, 2, 0, 0, false, nil, true},
		{"empty part 1 = no range", empty, 1, 0, 0, false, nil, false},
		{"empty part 2 exceeds", empty, 2, 0, 0, false, nil, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			start, length, isRange, count, err := partRange(tc.body, tc.part)
			if tc.wantErr {
				if !errors.Is(err, s3err.GetAPIError(s3err.ErrInvalidPartNumberRange)) {
					t.Fatalf("err = %v, want ErrInvalidPartNumberRange", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			if start != tc.wantStart || length != tc.wantLength || isRange != tc.wantRange {
				t.Fatalf("got (start=%d len=%d range=%v), want (start=%d len=%d range=%v)",
					start, length, isRange, tc.wantStart, tc.wantLength, tc.wantRange)
			}
			switch {
			case tc.wantCount == nil && count != nil:
				t.Fatalf("PartsCount = %d, want nil (non-multipart)", *count)
			case tc.wantCount != nil && count == nil:
				t.Fatalf("PartsCount = nil, want %d", *tc.wantCount)
			case tc.wantCount != nil && *count != *tc.wantCount:
				t.Fatalf("PartsCount = %d, want %d", *count, *tc.wantCount)
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
	if err != nil || start != 40 || length != 60 || !isRange || count == nil || *count != 2 {
		t.Fatalf("partNumber dispatch: start=%d len=%d range=%v count=%v err=%v", start, length, isRange, count, err)
	}

	// Range path: no partNumber, a byte range yields a nil parts-count.
	start, length, isRange, count, err = selectBytes(body, nil, "bytes=10-19")
	if err != nil || start != 10 || length != 10 || !isRange || count != nil {
		t.Fatalf("range dispatch: start=%d len=%d range=%v count=%v err=%v", start, length, isRange, count, err)
	}

	// No selector: whole object, not a range.
	start, length, isRange, count, err = selectBytes(body, nil, "")
	if err != nil || start != 0 || length != 100 || isRange || count != nil {
		t.Fatalf("whole-object dispatch: start=%d len=%d range=%v count=%v err=%v", start, length, isRange, count, err)
	}
}
