package config

import (
	"sync"
	"os"
	"time"
	"strconv"
)

var (
	once sync.Once
	cfg *Config
)
type Config struct {
	Port int
	Env string
	Capacity int
	TikerTime time.Duration
	Chan_cap int
	Sem_cap int
	NWorkers int
	Timeout time.Duration
}

func LoadConfig() *Config{
	once.Do(func() {
        cfg = defaults()
        applyEnv(cfg)
    })
    return cfg
}


func defaults() *Config {
	return &Config{
		Port:      8080,
		Env:       "dev",
		Capacity: 100,
		TikerTime: 1 * time.Microsecond,
		Chan_cap: 10,
		Sem_cap: 4,
		NWorkers:  5,
		Timeout: 100 * time.Millisecond,
	}
}
func applyEnv(c *Config) {
	envInt("TR_PORT", &c.Port)
	envStr("TR_ENV", &c.Env)
	envInt("TR_CAPACITY", &c.Capacity)
	envDur("TR_TICKER_TIME", &c.TikerTime)
	envInt("TR_CHAN_CAP", &c.Chan_cap)
	envInt("TR_SEM_CAP", &c.Sem_cap)
	envInt("TR_N_WORKERS", &c.NWorkers)
	envDur("TR_TIMEOUT", &c.Timeout)
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