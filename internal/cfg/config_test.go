package config

import (
	"sync"
	"testing"
	"time"
)

func TestLoadIsSingletonUnderRace(t *testing.T) {
	const n = 200

	var wg sync.WaitGroup
	got := make([]*Config, n)
	start := make(chan struct{})

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			c, err := Load()
			if err != nil {
				t.Errorf("Load: %v", err)
				return
			}
			got[i] = c
		}(i)
	}
	close(start)
	wg.Wait()

	first := got[0]
	if first == nil {
		t.Fatal("config did not load")
	}
	// Compared by pointer: that is what proves there is one instance.
	for i, c := range got {
		if c != first {
			t.Fatalf("goroutine %d got a different pointer: %p != %p", i, c, first)
		}
	}
}

func TestDefaultsAreValid(t *testing.T) {
	c := defaults()
	if err := c.Validate(); err != nil {
		t.Fatalf("default config is invalid: %v", err)
	}
}

func TestValidateCatchesBadValues(t *testing.T) {
	cases := map[string]func(*Config){
		"zero ticker_time":                 func(c *Config) { c.TickerTime = 0 },
		"zero workers":                     func(c *Config) { c.NWorkers = 0 },
		"zero semaphore":                   func(c *Config) { c.SemCap = 0 },
		"zero queue":                       func(c *Config) { c.ChanCap = 0 },
		"port out of range":                func(c *Config) { c.Port = 70000 },
		"unknown overflow":                 func(c *Config) { c.Overflow = "wait-a-bit" },
		"unknown mode":                     func(c *Config) { c.Mode = "magic" },
		"unknown store":                    func(c *Config) { c.Store = "redis" },
		"enqueue_timeout >= shutdown_http": func(c *Config) { c.EnqueueTimeout = 30 * time.Second },
	}

	for name, breakIt := range cases {
		t.Run(name, func(t *testing.T) {
			c := defaults()
			breakIt(&c)
			if err := c.Validate(); err == nil {
				t.Fatal("want a validation error")
			}
		})
	}
}

func TestEnvOverridesDefaults(t *testing.T) {
	t.Setenv("TR_N_WORKERS", "17")
	t.Setenv("TR_TOKEN_WAIT", "750ms")
	t.Setenv("TR_OVERFLOW", "block")

	c := defaults()
	applyEnv(&c)

	if c.NWorkers != 17 {
		t.Errorf("TR_N_WORKERS not applied: %d", c.NWorkers)
	}
	if c.TokenWait != 750*time.Millisecond {
		t.Errorf("TR_TOKEN_WAIT not applied: %v", c.TokenWait)
	}
	if c.Overflow != OverflowBlock {
		t.Errorf("TR_OVERFLOW not applied: %q", c.Overflow)
	}
}

func TestEnvGarbageKeepsDefault(t *testing.T) {
	t.Setenv("TR_N_WORKERS", "not-a-number")
	t.Setenv("TR_TOKEN_WAIT", "forever")

	c := defaults()
	wantWorkers, wantWait := c.NWorkers, c.TokenWait
	applyEnv(&c)

	if c.NWorkers != wantWorkers || c.TokenWait != wantWait {
		t.Fatalf("garbage overwrote defaults: workers=%d token_wait=%v", c.NWorkers, c.TokenWait)
	}
}
