package taskrunner_test

import (
	"errors"
	"testing"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/fil-forge/ingot/taskrunner"
)

// refusingProvider hands out a meter that refuses to create histograms.
type refusingProvider struct{ noop.MeterProvider }

func (refusingProvider) Meter(string, ...metric.MeterOption) metric.Meter { return refusingMeter{} }

type refusingMeter struct{ noop.Meter }

func (refusingMeter) Float64Histogram(string, ...metric.Float64HistogramOption) (metric.Float64Histogram, error) {
	return nil, errors.New("refused")
}

func TestMetricsTheProviderRefusesDoNotStopTheRunner(t *testing.T) {
	_, err := taskrunner.New(nil, taskrunner.Options{
		Schema:        "s",
		Queues:        []taskrunner.QueueOptions{{Name: "q"}},
		MeterProvider: refusingProvider{},
	})
	if err != nil {
		t.Errorf("New failed over a metrics problem: %v", err)
	}
}
