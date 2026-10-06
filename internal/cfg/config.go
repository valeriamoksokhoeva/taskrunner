package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	OverflowBlock  = "block"
	OverflowReject = "reject"
)

const (
	ModeHTTP  = "http"
	ModeSleep = "sleep"
)

const (
	StoreMutex   = "mutex"
	StoreSyncMap = "syncmap"
)

type Config struct {
	Port int
	Env  string

	NWorkers int
	ChanCap  int
	SemCap   int

	Capacity   int
	TickerTime time.Duration
	TokenWait  time.Duration

	EnqueueTimeout  time.Duration
	TaskTimeout     time.Duration
	SimulateFor     time.Duration
	ShutdownHTTP    time.Duration
	ShutdownWorkers time.Duration

	Overflow string
	Mode     string
	Store    string
}

var (
	once sync.Once
	cfg  *Config
	err  error
)

// Load builds the config once and returns the same instance to every caller.
func Load() (*Config, error) {
	once.Do(func() { cfg, err = build() })
	return cfg, err
}

func MustLoad() *Config {
	c, e := Load()
	if e != nil {
		panic(fmt.Sprintf("config: %v", e))
	}
	return c
}

func build() (*Config, error) {
	c := defaults()
	applyEnv(&c)
	if e := c.Validate(); e != nil {
		return nil, e
	}
	return &c, nil
}

func defaults() Config {
	return Config{
		Port: 8080,
		Env:  "dev",

		NWorkers: 8,
		ChanCap:  64,
		SemCap:   4,

		Capacity:   10,
		TickerTime: 100 * time.Millisecond,
		TokenWait:  500 * time.Millisecond,

		EnqueueTimeout:  5 * time.Second,
		TaskTimeout:     3 * time.Second,
		SimulateFor:     200 * time.Millisecond,
		ShutdownHTTP:    10 * time.Second,
		ShutdownWorkers: 15 * time.Second,

		Overflow: OverflowReject,
		Mode:     ModeSleep,
		Store:    StoreMutex,
	}
}

func applyEnv(c *Config) {
	envInt("TR_PORT", &c.Port)
	envStr("TR_ENV", &c.Env)

	envInt("TR_N_WORKERS", &c.NWorkers)
	envInt("TR_CHAN_CAP", &c.ChanCap)
	envInt("TR_SEM_CAP", &c.SemCap)

	envInt("TR_CAPACITY", &c.Capacity)
	envDur("TR_TICKER_TIME", &c.TickerTime)
	envDur("TR_TOKEN_WAIT", &c.TokenWait)

	envDur("TR_ENQUEUE_TIMEOUT", &c.EnqueueTimeout)
	envDur("TR_TASK_TIMEOUT", &c.TaskTimeout)
	envDur("TR_SIMULATE_FOR", &c.SimulateFor)
	envDur("TR_SHUTDOWN_HTTP", &c.ShutdownHTTP)
	envDur("TR_SHUTDOWN_WORKERS", &c.ShutdownWorkers)

	envStr("TR_OVERFLOW", &c.Overflow)
	envStr("TR_MODE", &c.Mode)
	envStr("TR_STORE", &c.Store)
}

func (c *Config) Validate() error {
	var errs []error

	if c.Port < 1 || c.Port > 65535 {
		errs = append(errs, fmt.Errorf("port=%d out of range 1..65535", c.Port))
	}
	if c.NWorkers < 1 {
		errs = append(errs, fmt.Errorf("n_workers=%d must be >= 1", c.NWorkers))
	}
	if c.ChanCap < 1 {
		errs = append(errs, fmt.Errorf("chan_cap=%d must be >= 1", c.ChanCap))
	}
	if c.SemCap < 1 {
		errs = append(errs, fmt.Errorf("sem_cap=%d must be >= 1", c.SemCap))
	}
	if c.Capacity < 1 {
		errs = append(errs, fmt.Errorf("capacity=%d must be >= 1", c.Capacity))
	}
	if c.TickerTime <= 0 {
		errs = append(errs, errors.New("ticker_time must be > 0, time.NewTicker panics otherwise"))
	}
	if c.TokenWait <= 0 {
		errs = append(errs, errors.New("token_wait must be > 0"))
	}
	if c.TaskTimeout <= 0 {
		errs = append(errs, errors.New("task_timeout must be > 0"))
	}
	if c.SimulateFor <= 0 {
		errs = append(errs, errors.New("simulate_for must be > 0"))
	}
	if c.Overflow != OverflowBlock && c.Overflow != OverflowReject {
		errs = append(errs, fmt.Errorf("overflow=%q, want %q or %q", c.Overflow, OverflowBlock, OverflowReject))
	}
	if c.Mode != ModeHTTP && c.Mode != ModeSleep {
		errs = append(errs, fmt.Errorf("mode=%q, want %q or %q", c.Mode, ModeHTTP, ModeSleep))
	}
	if c.Store != StoreMutex && c.Store != StoreSyncMap {
		errs = append(errs, fmt.Errorf("store=%q, want %q or %q", c.Store, StoreMutex, StoreSyncMap))
	}
	// A handler can block on enqueue for EnqueueTimeout, and the queues are
	// closed as soon as Shutdown returns.
	if c.EnqueueTimeout >= c.ShutdownHTTP {
		errs = append(errs, fmt.Errorf("enqueue_timeout=%v must be less than shutdown_http=%v",
			c.EnqueueTimeout, c.ShutdownHTTP))
	}

	return errors.Join(errs...)
}

func envStr(key string, dst *string) {
	if v, ok := os.LookupEnv(key); ok {
		*dst = v
	}
}

func envInt(key string, dst *int) {
	if v, ok := os.LookupEnv(key); ok {
		if n, e := strconv.Atoi(v); e == nil {
			*dst = n
		}
	}
}

func envDur(key string, dst *time.Duration) {
	if v, ok := os.LookupEnv(key); ok {
		if d, e := time.ParseDuration(v); e == nil {
			*dst = d
		}
	}
}
