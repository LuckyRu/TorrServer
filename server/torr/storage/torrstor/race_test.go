package torrstor

import (
	"sync/atomic"
	"testing"

	"github.com/anacrolix/torrent/metainfo"

	"server/settings"
	"server/torr/storage/state"
)

func testInfo() *metainfo.Info {
	return &metainfo.Info{
		Name:        "test",
		PieceLength: 1 << 10,
		Length:      1 << 20,
		Pieces:      make([]byte, (1<<10)*20),
	}
}

func initTestSettings(t *testing.T) {
	if settings.BTsets == nil {
		settings.BTsets = &settings.BTSets{}
		t.Cleanup(func() { settings.BTsets = nil })
	}
}

// Storage.caches: anacrolix-driven Cache.Close (delete) racing
// OpenTorrent/GetCache from other goroutines.
//
// The primary oracle is -race. The assertions below are the part that still means something
// without it: a cache handed out for a hash must be the cache of that hash, and a hash that
// was closed must not be handed out at all. Both would survive a torn map read silently.
func TestStorageCachesConcurrentOpenClose(t *testing.T) {
	initTestSettings(t)
	stor := NewStorage(1 << 20)

	const iters = 500
	var mismatched atomic.Int64

	startTogether(2, func(worker int) {
		for i := 0; i < iters; i++ {
			var h metainfo.Hash
			h[0] = byte(i)
			if worker == 0 {
				if _, err := stor.OpenTorrent(testInfo(), h); err != nil {
					t.Errorf("OpenTorrent: %v", err)
					return
				}
				if cache := stor.GetCache(h); cache != nil {
					if cache.hash != h {
						mismatched.Add(1)
					}
					// anacrolix calls TorrentImpl.Close from its own goroutine
					cache.Close()
				}
				continue
			}
			if cache := stor.GetCache(h); cache != nil && cache.hash != h {
				mismatched.Add(1)
			}
			var h2 metainfo.Hash
			h2[0] = byte(i)
			h2[1] = 1
			if _, err := stor.OpenTorrent(testInfo(), h2); err != nil {
				t.Errorf("OpenTorrent: %v", err)
				return
			}
			stor.CloseHash(h2)
			if cache := stor.GetCache(h2); cache != nil {
				mismatched.Add(1)
			}
		}
	})

	if got := mismatched.Load(); got != 0 {
		t.Fatalf("%d lookups returned a cache for the wrong hash or a closed one", got)
	}
}

// Cache.pieces: GetState/cleanPieces iterating while Close nils the map.
//
// -race is the primary oracle. The assertion that survives without it: a state snapshot taken
// while the cache is being closed must still describe that cache — a half-torn read shows up
// as a filled count larger than the capacity that was ever configured, or as a hash that is
// not the one asked for.
func TestCachePiecesConcurrentStateClose(t *testing.T) {
	initTestSettings(t)

	const iters = 300
	const capacity = 1 << 20

	for i := 0; i < iters; i++ {
		stor := NewStorage(capacity)
		var h metainfo.Hash
		h[0] = byte(i)
		if _, err := stor.OpenTorrent(testInfo(), h); err != nil {
			t.Fatalf("OpenTorrent: %v", err)
		}
		cache := stor.GetCache(h)
		if cache == nil {
			t.Fatal("GetCache returned nil for a torrent just opened")
		}

		var snapshot *state.CacheState
		startTogether(2, func(worker int) {
			if worker == 0 {
				snapshot = cache.GetState()
				cache.cleanPieces()
				return
			}
			cache.Close()
		})

		if snapshot == nil {
			t.Fatal("GetState returned nil")
		}
		if snapshot.Hash != h.HexString() {
			t.Fatalf("state describes hash %s, asked for %s", snapshot.Hash, h.HexString())
		}
		if snapshot.Filled < 0 || snapshot.Filled > snapshot.Capacity {
			t.Fatalf("filled=%d against capacity=%d", snapshot.Filled, snapshot.Capacity)
		}
	}
}
