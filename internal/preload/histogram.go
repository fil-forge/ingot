package preload

import (
	"math"
	"time"
)

const (
	// histSteps is the number of buckets per doubling of latency, so a
	// quantile is reported within about 4.4% (2^(1/16)) of the true value.
	histSteps = 16
	// histBuckets covers 1µs to about 2^31 µs (36 minutes); longer
	// latencies land in the last bucket.
	histBuckets = 31*histSteps + 2
)

// Histogram records latencies in logarithmic buckets. Its zero value is
// empty and ready to use. It is not safe for concurrent use.
type Histogram struct {
	counts [histBuckets]uint64
	n      uint64
	sum    time.Duration
	max    time.Duration
}

// Record adds one latency.
func (h *Histogram) Record(d time.Duration) {
	h.counts[histBucket(d)]++
	h.n++
	h.sum += d
	h.max = max(h.max, d)
}

// Merge adds every latency o recorded.
func (h *Histogram) Merge(o *Histogram) {
	for i, c := range o.counts {
		h.counts[i] += c
	}
	h.n += o.n
	h.sum += o.sum
	h.max = max(h.max, o.max)
}

// Count returns how many latencies were recorded.
func (h *Histogram) Count() uint64 { return h.n }

// Max returns the longest latency recorded.
func (h *Histogram) Max() time.Duration { return h.max }

// Mean returns the mean latency, or 0 when none was recorded.
func (h *Histogram) Mean() time.Duration {
	if h.n == 0 {
		return 0
	}
	return h.sum / time.Duration(h.n)
}

// Quantile returns the latency at quantile q (0 < q ≤ 1): the upper bound of
// the bucket holding it, capped at the longest latency recorded, or that
// latency itself when it lies past the last bucket. It returns 0 when none
// was recorded.
func (h *Histogram) Quantile(q float64) time.Duration {
	if h.n == 0 {
		return 0
	}
	rank := uint64(math.Ceil(q * float64(h.n)))
	rank = max(rank, 1)
	var seen uint64
	for i, c := range h.counts {
		seen += c
		if seen >= rank {
			if i == histBuckets-1 {
				return h.max
			}
			return min(histUpper(i), h.max)
		}
	}
	return h.max
}

// histBucket returns the bucket holding d: bucket 0 holds latencies under
// 1µs, and bucket i ≥ 1 those above bucket i-1's and up to
// 2^((i-1)/histSteps) µs.
func histBucket(d time.Duration) int {
	us := float64(d) / float64(time.Microsecond)
	if us < 1 {
		return 0
	}
	i := int(math.Ceil(math.Log2(us)*histSteps)) + 1
	return min(max(i, 1), histBuckets-1)
}

// histUpper returns the longest latency bucket i holds.
func histUpper(i int) time.Duration {
	if i == 0 {
		return time.Microsecond
	}
	return time.Duration(math.Exp2(float64(i-1)/histSteps) * float64(time.Microsecond))
}

// LatencySummary is a Histogram's quantiles, as the metrics file records
// them.
type LatencySummary struct {
	Count uint64  `json:"count"`
	Mean  float64 `json:"mean_ms"`
	P50   float64 `json:"p50_ms"`
	P90   float64 `json:"p90_ms"`
	P99   float64 `json:"p99_ms"`
	P999  float64 `json:"p999_ms"`
	Max   float64 `json:"max_ms"`
}

// Summary returns h's quantiles in milliseconds.
func (h *Histogram) Summary() LatencySummary {
	return LatencySummary{
		Count: h.n,
		Mean:  ms(h.Mean()),
		P50:   ms(h.Quantile(0.5)),
		P90:   ms(h.Quantile(0.9)),
		P99:   ms(h.Quantile(0.99)),
		P999:  ms(h.Quantile(0.999)),
		Max:   ms(h.max),
	}
}

func ms(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}
