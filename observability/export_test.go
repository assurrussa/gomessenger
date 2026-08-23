package observability

import "github.com/prometheus/client_golang/prometheus"

// Operations returns the operations counter vector for testing.
func (o *Observer) Operations() *prometheus.CounterVec { return o.operations }

// Duration returns the duration histogram vector for testing.
func (o *Observer) Duration() *prometheus.HistogramVec { return o.duration }
