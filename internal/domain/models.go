package domain

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

type Priority string
const (
	PriorityHigh Priority = "high"
	PriorityLow Priority = "low"
)
type TaskResult struct {
	Status string
	Result string
}

type Store struct {
	items map[string]TaskResult
	rwm sync.RWMutex
}

type Task struct {
	ID string
	Payload string
}

type Metrics struct {
	processed atomic.Int64
	failed atomic.Int64
	workers atomic.Int64
}

type TokenBucket struct {
	tokens chan struct{}
	ticker  *time.Ticker
}

type Service struct {
	tb *TokenBucket
	store Storage
	m *Metrics
	highCh chan Task
	lowCh chan Task
	sem chan struct{}
	n_workers int
	timeout time.Duration
	wg sync.WaitGroup
	closeOnce sync.Once
}

type ServiceSample struct {
	Capacity int
	TikerTime time.Duration
	Chan_cap int
	Sem_cap int
	N_workers int
	Timeout time.Duration
}

type Storage interface {
	Set(key string, value TaskResult)
	Get(key string) (TaskResult, bool)
	Delete(key string)
}

type StoreSync struct {
	m sync.Map
}

type MetricsRes struct {
	Processed int64
	Failed int64 
	ActiveWorkers int64 
	AverageProcessTime int64 
} 
func NewService(ctx context.Context, sample ServiceSample) *Service {
	tb := NewTokenBucket(ctx, sample.Capacity, sample.TikerTime)
	s := NewStore()
	high := make(chan Task, sample.Chan_cap)
	low := make(chan Task, sample.Chan_cap)
	sem := make(chan struct{}, sample.Sem_cap)
	m := NewMetrics()
	return &Service{tb: tb, 
		store: s, 
		m: m, 
		highCh: high, 
		lowCh: low, 
		sem: sem, 
		n_workers: sample.N_workers,  
		timeout: sample.Timeout,
		wg: sync.WaitGroup{},
	}
}