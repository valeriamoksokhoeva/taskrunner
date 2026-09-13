package domain


func NewMetrics() *Metrics {
	return &Metrics{}
}

func (s *Metrics) IncProcessed() {
	s.processed.Add(1)
}

func (s *Metrics) IncFailed() {
	s.failed.Add(1)
}

func (s *Metrics) Snapshot() (int64, int64, int64){
	return s.processed.Load(), s.failed.Load(), s.workers.Load()
}

func (s *Metrics) IncWorkers() {
	s.workers.Add(1)
}

func (s *Metrics) DecWorkers() {
	s.workers.Add(-1)
}
