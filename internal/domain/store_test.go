package domain

import (
	"fmt"
	"sync"
	"testing"
)

func storages() map[string]func() Storage {
	return map[string]func() Storage{
		"RWMutex": func() Storage { return NewStore() },
		"SyncMap": func() Storage { return NewStoreSync() },
	}
}

func TestStorageBasics(t *testing.T) {
	for name, newStorage := range storages() {
		t.Run(name, func(t *testing.T) {
			s := newStorage()

			if _, ok := s.Get("missing"); ok {
				t.Fatal("Get on empty storage returned ok=true")
			}

			s.Set("a", TaskResult{Status: StatusDone, Result: "ok"})
			got, ok := s.Get("a")
			if !ok || got.Status != StatusDone || got.Result != "ok" {
				t.Fatalf("got %+v ok=%v", got, ok)
			}

			s.Set("a", TaskResult{Status: StatusFailed, Result: "boom"})
			if got, _ := s.Get("a"); got.Status != StatusFailed {
				t.Fatalf("overwrite failed: %+v", got)
			}

			s.Delete("a")
			if _, ok := s.Get("a"); ok {
				t.Fatal("key still present after Delete")
			}
		})
	}
}

func TestStorageConcurrent(t *testing.T) {
	const (
		writers = 16
		readers = 32
		ops     = 500
	)

	for name, newStorage := range storages() {
		t.Run(name, func(t *testing.T) {
			s := newStorage()
			var wg sync.WaitGroup
			start := make(chan struct{})

			for w := 0; w < writers; w++ {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					<-start
					for i := 0; i < ops; i++ {
						s.Set(fmt.Sprintf("k-%d", i%64), TaskResult{Status: StatusDone, Result: fmt.Sprint(w, i)})
					}
				}(w)
			}

			for r := 0; r < readers; r++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					for i := 0; i < ops; i++ {
						s.Get(fmt.Sprintf("k-%d", i%64))
					}
				}()
			}

			for d := 0; d < 4; d++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					for i := 0; i < ops; i++ {
						s.Delete(fmt.Sprintf("k-%d", i%64))
					}
				}()
			}

			close(start)
			wg.Wait()
		})
	}
}
