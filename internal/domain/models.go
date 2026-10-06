package domain

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

type Priority string

const (
	PriorityHigh Priority = "high"
	PriorityLow  Priority = "low"
)

const (
	StatusPending   = "pending"
	StatusDone      = "done"
	StatusFailed    = "failed"
	StatusCancelled = "cancelled"
)

type Task struct {
	ID      string
	Payload string
}

type TaskResult struct {
	Status string
	Result string
}

type Storage interface {
	Set(key string, value TaskResult)
	Get(key string) (TaskResult, bool)
	Delete(key string)
}

type Store struct {
	items map[string]TaskResult
	rwm   sync.RWMutex
}

type StoreSync struct {
	m sync.Map
}

type Metrics struct {
	processed  atomic.Int64
	failed     atomic.Int64
	cancelled  atomic.Int64
	workers    atomic.Int64
	totalNanos atomic.Int64
}

type MetricsRes struct {
	Processed          int64
	Failed             int64
	Cancelled          int64
	ActiveWorkers      int64
	AverageProcessTime int64
}

type TokenBucket struct {
	tokens chan struct{}
	ticker *time.Ticker
}

type Service struct {
	tb    *TokenBucket
	store Storage
	m     *Metrics

	highCh chan Task
	lowCh  chan Task
	sem    chan struct{}

	numWorkers  int
	tokenWait   time.Duration
	taskTimeout time.Duration
	simulateFor time.Duration
	overflow    string
	mode        string
	client      *http.Client

	wg        sync.WaitGroup
	closeOnce sync.Once
}

type ServiceSample struct {
	Capacity    int
	TickerTime  time.Duration
	ChanCap     int
	SemCap      int
	NWorkers    int
	TokenWait   time.Duration
	TaskTimeout time.Duration
	SimulateFor time.Duration
	Overflow    string
	Mode        string
	Store       string
}

func NewService(ctx context.Context, sample ServiceSample) *Service {
	return &Service{
		tb:    NewTokenBucket(ctx, sample.Capacity, sample.TickerTime),
		store: NewStorage(sample.Store),
		m:     NewMetrics(),

		highCh: make(chan Task, sample.ChanCap),
		lowCh:  make(chan Task, sample.ChanCap),
		sem:    make(chan struct{}, sample.SemCap),

		numWorkers:  sample.NWorkers,
		tokenWait:   sample.TokenWait,
		taskTimeout: sample.TaskTimeout,
		simulateFor: sample.SimulateFor,
		overflow:    sample.Overflow,
		mode:        sample.Mode,
		client: &http.Client{
			Timeout:   sample.TaskTimeout,
			Transport: &http.Transport{MaxIdleConnsPerHost: sample.SemCap},
		},
	}
}
