package preload

import (
	"encoding/json"
	"fmt"
	"slices"
	"time"
)

// Report is a load's progress at one moment, as the metrics file records it
// a line at a time.
type Report struct {
	Time time.Time `json:"time"`
	// ElapsedS is the time since the load started, and IntervalS the time
	// since the previous report, in seconds.
	ElapsedS  float64 `json:"elapsed_s"`
	IntervalS float64 `json:"interval_s"`
	// Final marks the report a load ends with.
	Final   bool                    `json:"final,omitempty"`
	Total   Totals                  `json:"total"`
	Buckets map[string]BucketReport `json:"buckets"`
	// Latency is keyed by operation (OpPutBulk, OpPutReal). It times PUTs
	// that stored an object, not those that found their key taken.
	Latency   map[string]Latency `json:"latency"`
	LastError string             `json:"last_error,omitempty"`
}

// Totals sums the buckets' figures.
type Totals struct {
	// Written and Existed count this load's PUTs that stored an object and
	// that found their key already taken; both count as written.
	Written uint64 `json:"written"`
	Existed uint64 `json:"existed"`
	// Errors counts failed attempts, each tried again.
	Errors uint64 `json:"errors"`
	// Real counts the real objects the buckets hold, earlier loads'
	// included, and RealBytes the bytes of those this load wrote.
	Real      uint64 `json:"real"`
	RealBytes uint64 `json:"real_bytes"`
	// Rate is objects written per second over the interval, and
	// OverallRate over the whole load.
	Rate        float64 `json:"rate"`
	OverallRate float64 `json:"overall_rate"`
}

// BucketReport is one bucket's figures.
type BucketReport struct {
	// Bulk is the bucket's bulk watermark: every bulk object below it is
	// written.
	Bulk    uint64  `json:"bulk"`
	Written uint64  `json:"written"`
	Existed uint64  `json:"existed"`
	Errors  uint64  `json:"errors"`
	Real    uint64  `json:"real"`
	Rate    float64 `json:"rate"`
}

// Latency is an operation's latencies over the interval and the whole load.
type Latency struct {
	Interval LatencySummary `json:"interval"`
	Total    LatencySummary `json:"total"`
}

// report builds a report, writes it to the metrics file and the progress
// writer, and starts the next interval.
func (l *loader) report(final bool) *Report {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	interval := now.Sub(l.lastReport).Seconds()
	elapsed := now.Sub(l.start).Seconds()
	r := &Report{
		Time:      now.UTC(),
		ElapsedS:  elapsed,
		IntervalS: interval,
		Final:     final,
		Buckets:   make(map[string]BucketReport, len(l.buckets)),
		Latency:   make(map[string]Latency, len(l.hist.total)),
		LastError: l.lastErr,
	}
	var delta uint64
	for _, b := range l.buckets {
		br := BucketReport{
			Bulk:    b.wm.Done(),
			Written: b.written.Load(),
			Existed: b.existed.Load(),
			Errors:  b.errors.Load(),
			Real:    b.real.Load(),
		}
		puts := br.Written + br.Existed
		if interval > 0 {
			br.Rate = float64(puts-b.prev) / interval
		}
		delta += puts - b.prev
		b.prev = puts
		r.Buckets[b.name] = br
		r.Total.Written += br.Written
		r.Total.Existed += br.Existed
		r.Total.Errors += br.Errors
		r.Total.Real += br.Real
	}
	r.Total.RealBytes = l.realBytes.Load()
	if interval > 0 {
		r.Total.Rate = float64(delta) / interval
	}
	if elapsed > 0 {
		r.Total.OverallRate = float64(r.Total.Written+r.Total.Existed) / elapsed
	}
	for op, total := range l.hist.total {
		lat := Latency{Total: total.Summary()}
		if h, ok := l.hist.interval[op]; ok {
			lat.Interval = h.Summary()
		}
		r.Latency[op] = lat
	}
	clear(l.hist.interval)
	l.lastReport = now

	if l.cfg.Metrics != nil {
		if b, err := json.Marshal(r); err == nil {
			l.cfg.Metrics.Write(append(b, '\n'))
		}
	}
	if l.cfg.Progress != nil {
		fmt.Fprintln(l.cfg.Progress, l.progressLine(r))
	}
	return r
}

// progressLine is r in one line of text.
func (l *loader) progressLine(r *Report) string {
	bulks := make([]uint64, 0, len(r.Buckets))
	for _, b := range r.Buckets {
		bulks = append(bulks, b.Bulk)
	}
	var due uint64
	if l.cfg.RealEvery > 0 {
		for _, b := range l.buckets {
			due += b.wm.limit / l.cfg.RealEvery
		}
	}
	elapsed := time.Duration(r.ElapsedS * float64(time.Second)).Round(time.Second)
	line := fmt.Sprintf("%8s  %d written, %.0f/s (%.0f/s overall)  bulk per bucket %d–%d of %d  real %d of %d  errors %d",
		elapsed, r.Total.Written+r.Total.Existed, r.Total.Rate, r.Total.OverallRate,
		slices.Min(bulks), slices.Max(bulks), l.cfg.Until, r.Total.Real, due, r.Total.Errors)
	for _, op := range []string{OpPutBulk, OpPutReal} {
		if lat, ok := r.Latency[op]; ok && lat.Interval.Count > 0 {
			line += fmt.Sprintf("  %s p50 %.1fms p99 %.1fms", op, lat.Interval.P50, lat.Interval.P99)
		}
	}
	if r.Final {
		line += "  (final)"
	}
	return line
}
