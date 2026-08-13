package torrstor

import (
	"errors"
	"sync"
	"testing"
)

func TestAdmitReaderRefusesPastTheLimit(t *testing.T) {
	cache := &Cache{readers: make(map[*Reader]struct{})}

	held := make([]*Reader, 0, maxReadersPerTorrent)
	for range maxReadersPerTorrent {
		reader := &Reader{cache: cache}
		if err := cache.admitReader(reader); err != nil {
			t.Fatalf("admitReader below the limit: %v", err)
		}
		held = append(held, reader)
	}
	if cache.Readers() != maxReadersPerTorrent {
		t.Fatalf("Readers()=%d, want %d", cache.Readers(), maxReadersPerTorrent)
	}

	if err := cache.admitReader(&Reader{cache: cache}); !errors.Is(err, ErrTooManyReaders) {
		t.Fatalf("admitReader past the limit error=%v, want ErrTooManyReaders", err)
	}
	if cache.Readers() != maxReadersPerTorrent {
		t.Fatal("a refused reader was published anyway")
	}

	// Releasing one frees a slot. Removing it from the map is what CloseReader does to
	// release it; the rest of CloseReader needs a live torrent behind the reader.
	cache.muReaders.Lock()
	delete(cache.readers, held[0])
	cache.muReaders.Unlock()

	if err := cache.admitReader(&Reader{cache: cache}); err != nil {
		t.Fatalf("admitReader after a release: %v", err)
	}
}

// Callers arrive from independent HTTP requests, so the check and the publish have to be
// one step — otherwise every one of them passes a check against the same stale count.
func TestAdmitReaderDoesNotOvershootUnderConcurrency(t *testing.T) {
	cache := &Cache{readers: make(map[*Reader]struct{})}

	const callers = maxReadersPerTorrent * 4
	var wait sync.WaitGroup
	var mu sync.Mutex
	admitted := 0

	wait.Add(callers)
	for range callers {
		go func() {
			defer wait.Done()
			if err := cache.admitReader(&Reader{cache: cache}); err == nil {
				mu.Lock()
				admitted++
				mu.Unlock()
			}
		}()
	}
	wait.Wait()

	if admitted != maxReadersPerTorrent {
		t.Fatalf("admitted %d readers, want exactly %d", admitted, maxReadersPerTorrent)
	}
	if cache.Readers() != maxReadersPerTorrent {
		t.Fatalf("Readers()=%d, want %d", cache.Readers(), maxReadersPerTorrent)
	}
}
