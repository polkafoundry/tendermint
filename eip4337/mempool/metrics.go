package mempool

import (
	"github.com/go-kit/kit/metrics"
	"github.com/go-kit/kit/metrics/discard"
	"github.com/go-kit/kit/metrics/prometheus"
	stdprometheus "github.com/prometheus/client_golang/prometheus"
)

const (
	// MetricsSubsystem is a subsystem shared by all metrics exposed by this
	// package.
	MetricsSubsystem = "mempool"
)

// Metrics contains metrics exposed by this package.
// see MetricsProvider for descriptions.
type Metrics struct {
	// Size of the mempool.
	Size metrics.Gauge

	// Histogram of transaction sizes, in bytes.
	OpSizeBytes metrics.Histogram

	// Number of failed transactions.
	FailedOps metrics.Counter

	// RejectedOps defines the number of rejected transactions. These are
	// transactions that passed CheckOp but failed to make it into the mempool
	// due to resource limits, e.g. mempool is full and no lower priority
	// transactions exist in the mempool.
	RejectedOps metrics.Counter

	// EvictedOps defines the number of evicted transactions. These are valid
	// transactions that passed CheckOp and existed in the mempool but were later
	// evicted to make room for higher priority valid transactions that passed
	// CheckOp.
	EvictedOps metrics.Counter

	// Number of times transactions are rechecked in the mempool.
	RecheckTimes metrics.Counter
}

// PrometheusMetrics returns Metrics build using Prometheus client library.
// Optionally, labels can be provided along with their values ("foo",
// "fooValue").
func PrometheusMetrics(namespace string, labelsAndValues ...string) *Metrics {
	labels := []string{}
	for i := 0; i < len(labelsAndValues); i += 2 {
		labels = append(labels, labelsAndValues[i])
	}
	return &Metrics{
		Size: prometheus.NewGaugeFrom(stdprometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: MetricsSubsystem,
			Name:      "size",
			Help:      "Size of the mempool (number of uncommitted transactions).",
		}, labels).With(labelsAndValues...),

		OpSizeBytes: prometheus.NewHistogramFrom(stdprometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: MetricsSubsystem,
			Name:      "op_size_bytes",
			Help:      "Transaction sizes in bytes.",
			Buckets:   stdprometheus.ExponentialBuckets(1, 3, 17),
		}, labels).With(labelsAndValues...),

		FailedOps: prometheus.NewCounterFrom(stdprometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: MetricsSubsystem,
			Name:      "failed_ops",
			Help:      "Number of failed transactions.",
		}, labels).With(labelsAndValues...),

		RejectedOps: prometheus.NewCounterFrom(stdprometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: MetricsSubsystem,
			Name:      "rejected_ops",
			Help:      "Number of rejected transactions.",
		}, labels).With(labelsAndValues...),

		EvictedOps: prometheus.NewCounterFrom(stdprometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: MetricsSubsystem,
			Name:      "evicted_ops",
			Help:      "Number of evicted transactions.",
		}, labels).With(labelsAndValues...),

		RecheckTimes: prometheus.NewCounterFrom(stdprometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: MetricsSubsystem,
			Name:      "recheck_times",
			Help:      "Number of times transactions are rechecked in the mempool.",
		}, labels).With(labelsAndValues...),
	}
}

// NopMetrics returns no-op Metrics.
func NopMetrics() *Metrics {
	return &Metrics{
		Size:         discard.NewGauge(),
		OpSizeBytes:  discard.NewHistogram(),
		FailedOps:    discard.NewCounter(),
		RejectedOps:  discard.NewCounter(),
		EvictedOps:   discard.NewCounter(),
		RecheckTimes: discard.NewCounter(),
	}
}
