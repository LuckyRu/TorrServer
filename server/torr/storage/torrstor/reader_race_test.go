package torrstor

import (
	"sync"
	"testing"
)

// isUse is written under Reader.mu by the goroutine serving one stream, and read under
// Cache.muReaders by the cache maintenance that runs for every other stream. Two different
// locks guard the same field, which is no lock at all.
//
// One reader per torrent hid this: with a task per session there are several, and the
// maintenance pass walks all of them while each is being served.
func TestReaderUseFlagIsNotRacedByCacheMaintenance(t *testing.T) {
	cache := &Cache{readers: make(map[*Reader]struct{})}
	readers := make([]*Reader, 0, 4)
	for range 4 {
		r := &Reader{cache: cache}
		r.isUse.Store(true)
		cache.readers[r] = struct{}{}
		readers = append(readers, r)
	}

	var wait sync.WaitGroup
	stop := make(chan struct{})

	// The maintenance side: counting active readers is what sizes the cache and splits the
	// connection budget.
	wait.Add(1)
	go func() {
		defer wait.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_ = cache.GetUseReaders()
			}
		}
	}()

	// The serving side: a reader goes idle and comes back as its viewer pauses and resumes.
	for _, r := range readers {
		wait.Add(1)
		go func(reader *Reader) {
			defer wait.Done()
			for i := range 2000 {
				reader.isUse.Store(i%2 == 0)
			}
		}(r)
	}

	for range 2000 {
		_ = cache.GetUseReaders()
	}
	close(stop)
	wait.Wait()
}
