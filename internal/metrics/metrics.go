package metrics

import (
	"net/http"

	. "github.com/voyvodov/goofys/api/common"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type GoofysMetrics struct {
	registry *prometheus.Registry
}

var log = GetLogger("metrics")

func NewGoofysMetrics() *GoofysMetrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		newGoofysCollector(),
	)

	return &GoofysMetrics{
		registry: reg,
	}
}

func (m *GoofysMetrics) Start(addr string) {
	http.Handle("/metrics", promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{}))

	s := &http.Server{Addr: addr}
	go func() {
		if err := s.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			// Log the error, but don't crash the application
			log.Errorf("Error starting metrics server: %v", err)
		}
	}()

	log.Infof("Metrics server started on %s", addr)
}
