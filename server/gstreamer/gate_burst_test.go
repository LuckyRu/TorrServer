//go:build gst

package gstreamer

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// startTogether releases every caller from one point. The defect this file is about only shows
// up when the callers arrive at once — released one at a time they queue politely, which is
// exactly why the existing staged-degradation test passes on the burst-prone code.
func startTogether(callers int, body func(int)) {
	var wait sync.WaitGroup
	ready := make(chan struct{}, callers)
	start := make(chan struct{})

	wait.Add(callers)
	for i := range callers {
		go func(index int) {
			defer wait.Done()
			ready <- struct{}{}
			<-start
			body(index)
		}(i)
	}
	for range callers {
		<-ready
	}
	close(start)
	wait.Wait()
}

// Giving up after a timeout and going ahead anyway is right for one caller and wrong for
// several: they all reach the timeout at about the same moment and start together, so the
// serialisation disappears exactly under the load it exists for. A household starting several
// streams on one release is that load.
//
// The property under test is not "some callers wait" but "the ones who give up do not all give
// up in the same instant".
func TestTorrentGateStaggersABurst(t *testing.T) {
	service := NewService(DefaultConfig())
	t.Cleanup(service.Dispose)

	// Hold the single turn for the whole test, so every caller below has to degrade.
	held := service.acquireTorrentWithin("hash", time.Second)
	t.Cleanup(held)

	const wait = 60 * time.Millisecond
	const callers = 8
	// Anything that got through in appreciably less than two rounds did not sit out the second
	// one. Measured per caller from its own release, so goroutine startup is not in the number.
	const firstRound = wait * 3 / 2

	var releases [callers]func()
	var wentAheadInFirstRound atomic.Int64

	startTogether(callers, func(index int) {
		begin := time.Now()
		release := service.acquireTorrentWithin("hash", wait)
		if time.Since(begin) < firstRound {
			wentAheadInFirstRound.Add(1)
		}
		releases[index] = release
	})
	t.Cleanup(func() {
		for _, release := range releases {
			if release != nil {
				release()
			}
		}
	})

	// Only the ungated slots may be handed out in the first round. Everyone else has to sit
	// through a second one — that is what spreads the starts out. The old code let all of them
	// through at the same moment, which is the burst this exists to prevent.
	if got := wentAheadInFirstRound.Load(); got > int64(maxUngatedTorrentOps) {
		t.Fatalf("%d of %d callers went ahead in the first round; at most %d may skip the queue",
			got, callers, maxUngatedTorrentOps)
	}
}

// The ungated slots are a bounded escape hatch, not an unbounded one: they have to be handed
// back, or the first burst uses them up forever and every later caller waits the full two
// rounds for nothing.
func TestUngatedSlotsAreReturned(t *testing.T) {
	service := NewService(DefaultConfig())
	t.Cleanup(service.Dispose)

	held := service.acquireTorrentWithin("hash", 50*time.Millisecond)

	releases := make([]func(), 0, maxUngatedTorrentOps)
	for range maxUngatedTorrentOps {
		releases = append(releases, service.acquireTorrentWithin("hash", 20*time.Millisecond))
	}
	for _, release := range releases {
		release()
	}
	held()

	// With everything handed back, the very next caller must get the turn immediately.
	done := make(chan struct{})
	go func() {
		release := service.acquireTorrentWithin("hash", 2*time.Second)
		release()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("the gate never recovered after a burst returned its slots")
	}
}
