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

// The difference between a competitor and a companion is where it is reading. A preload
// warms the head of a file, which is what a probe reads too — yielding to that starved the
// probe of the very bytes being fetched for it.
func TestHasReaderPastSeparatesCompetitorsFromCompanions(t *testing.T) {
	cache := &Cache{readers: make(map[*Reader]struct{})}

	atHead := &Reader{cache: cache}
	atHead.isUse.Store(true)
	atHead.offset.Store(1 << 20)
	cache.readers[atHead] = struct{}{}

	const covered = 32 << 20
	if cache.HasReaderPast(covered) {
		t.Fatal("a reader inside the warmed range wants the same bytes, not other ones")
	}

	farAway := &Reader{cache: cache}
	farAway.isUse.Store(true)
	farAway.offset.Store(covered * 4)
	cache.readers[farAway] = struct{}{}

	if !cache.HasReaderPast(covered) {
		t.Fatal("a reader past the warmed range wants other bytes and should be noticed")
	}

	// An idle reader is nobody waiting.
	farAway.isUse.Store(false)
	if cache.HasReaderPast(covered) {
		t.Fatal("an idle reader must not count as a competitor")
	}
}
