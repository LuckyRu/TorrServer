package torr

import (
	"testing"

	"server/settings"
	"server/torr/storage/torrstor"
)

// The per-reader floor deliberately lets the readers of one torrent ask for more connections
// than ConnectionsLimit. That only holds if the client is configured for the larger number,
// and it reads EstablishedConnsPerTorrent once, at startup. A budget below what the floor
// promises does not fail loudly; it caps the floor silently, at the exact moment a household
// is watching the same release together.
func TestEstablishedConnsCoverTheReaderFloor(t *testing.T) {
	previous := settings.BTsets
	t.Cleanup(func() { settings.BTsets = previous })

	needed := torrstor.MaxConnectionsNeeded()
	if needed <= 0 {
		t.Fatalf("MaxConnectionsNeeded=%d, the floor promises nothing", needed)
	}

	for _, configured := range []int{25, 60, needed * 2} {
		settings.BTsets = &settings.BTSets{ConnectionsLimit: configured}

		if got := establishedConnsPerTorrent(); got < needed {
			t.Fatalf("ConnectionsLimit=%d gives a budget of %d, below the %d the reader floor promises",
				configured, got, needed)
		}
		// A deliberately generous setting is the operator's call and must survive.
		if configured > needed {
			if got := establishedConnsPerTorrent(); got != configured {
				t.Fatalf("ConnectionsLimit=%d was lowered to %d", configured, got)
			}
		}
	}
}
