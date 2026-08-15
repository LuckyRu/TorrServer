package torr

import (
	"sync/atomic"
	"testing"

	"github.com/anacrolix/torrent/metainfo"
)

// Mimics the real writers (NewTorrent insert, Torrent.Close delete), which
// hold bt.mu, racing against the public read methods used by HTTP handlers.
//
// -race is the primary oracle. What still means something without it: ListTorrents must hand
// back its own map, not a live view of bt.torrents — a caller iterating the returned map while
// a torrent is added or removed would otherwise crash the HTTP handler outright.
func TestBTServerTorrentsConcurrentAccess(t *testing.T) {
	bt := NewBTS()

	const iters = 5000
	var wrongHash atomic.Int64

	startTogether(2, func(worker int) {
		var h metainfo.Hash
		for i := 0; i < iters; i++ {
			h[0] = byte(i)
			h[1] = byte(i >> 8)
			if worker == 0 {
				bt.mu.Lock()
				bt.torrents[h] = &Torrent{}
				bt.mu.Unlock()
				bt.mu.Lock()
				delete(bt.torrents, h)
				bt.mu.Unlock()
				continue
			}
			bt.GetTorrent(h)
			listed := bt.ListTorrents()
			// Iterating the snapshot must be safe on its own; a shared map would fault here
			// under the writer above rather than merely report a race.
			for hash := range listed {
				if _, ok := listed[hash]; !ok {
					wrongHash.Add(1)
				}
			}
		}
	})

	if got := wrongHash.Load(); got != 0 {
		t.Fatalf("%d entries vanished from a snapshot while it was being read", got)
	}
}
