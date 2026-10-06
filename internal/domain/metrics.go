package domain

import "time"

func NewMetrics() *Metrics { return &Metrics{} }

func (m *Metrics) IncProcessed() { m.processed.Add(1) }
func (m *Metrics) IncFailed()    { m.failed.Add(1) }
func (m *Metrics) IncCancelled() { m.cancelled.Add(1) }
func (m *Metrics) IncWorkers()   { m.workers.Add(1) }
func (m *Metrics) DecWorkers()   { m.workers.Add(-1) }

func (m *Metrics) AddDuration(d time.Duration) { m.totalNanos.Add(int64(d)) }

func (m *Metrics) Snapshot() MetricsRes {
	processed := m.processed.Load()
	var avgMs int64
	if processed > 0 {
		avgMs = (m.totalNanos.Load() / processed) / int64(time.Millisecond)
	}
	return MetricsRes{
		Processed:          processed,
		Failed:             m.failed.Load(),
		Cancelled:          m.cancelled.Load(),
		ActiveWorkers:      m.workers.Load(),
		AverageProcessTime: avgMs,
	}
}
