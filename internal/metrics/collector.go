package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	GoofysRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "goofys_request_duration_seconds",
			Help:    "Duration of requests handled by goofys",
			Buckets: prometheus.ExponentialBuckets(0.1, 1.5, 10), // 100ms to ~38s
		},
		[]string{"operation", "backend", "status"},
	)
	GoofysRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "goofys_requests_total",
			Help: "Total number of requests handled by goofys",
		},
		[]string{"operation", "backend", "status"},
	)
)

type GoofysCollector struct {
	// Add any fields you need for collecting metrics

}

func newGoofysCollector() *GoofysCollector {
	return &GoofysCollector{}
}

func (c *GoofysCollector) Describe(ch chan<- *prometheus.Desc) {
	// Describe the metrics you want to collect
}

func (c *GoofysCollector) Collect(ch chan<- prometheus.Metric) {
	// Collect the metrics and send them to the channel

	GoofysRequestDuration.Collect(ch)
	GoofysRequestsTotal.Collect(ch)
}
