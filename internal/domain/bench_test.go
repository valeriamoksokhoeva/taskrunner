package domain

import (
	"fmt"
	"math/rand"
	"testing"
)

// go test ./internal/domain -bench=BenchmarkStorage -benchmem
//
// sync.Map wins on an almost read-only key set and loses as soon as writes
// are a real share of the load, where every Store boxes the value and
// touches the dirty map. This service does two writes per task plus some
// status reads, which lands near the 90% mark, so RWMutex is the default.
func benchStorage(b *testing.B, s Storage, readPercent int) {
	const keys = 1024
	for i := 0; i < keys; i++ {
		s.Set(fmt.Sprintf("k-%d", i), TaskResult{Status: StatusPending})
	}

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		rnd := rand.New(rand.NewSource(rand.Int63()))
		for pb.Next() {
			k := fmt.Sprintf("k-%d", rnd.Intn(keys))
			if rnd.Intn(100) < readPercent {
				s.Get(k)
			} else {
				s.Set(k, TaskResult{Status: StatusDone, Result: "r"})
			}
		}
	})
}

func BenchmarkStorageRWMutex_Read100(b *testing.B) { benchStorage(b, NewStore(), 100) }
func BenchmarkStorageSyncMap_Read100(b *testing.B) { benchStorage(b, NewStoreSync(), 100) }
func BenchmarkStorageRWMutex_Read90(b *testing.B)  { benchStorage(b, NewStore(), 90) }
func BenchmarkStorageSyncMap_Read90(b *testing.B)  { benchStorage(b, NewStoreSync(), 90) }
func BenchmarkStorageRWMutex_Read50(b *testing.B)  { benchStorage(b, NewStore(), 50) }
func BenchmarkStorageSyncMap_Read50(b *testing.B)  { benchStorage(b, NewStoreSync(), 50) }
func BenchmarkStorageRWMutex_Read10(b *testing.B)  { benchStorage(b, NewStore(), 10) }
func BenchmarkStorageSyncMap_Read10(b *testing.B)  { benchStorage(b, NewStoreSync(), 10) }
