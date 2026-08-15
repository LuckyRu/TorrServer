package torr

import (
	"math"
	"sync"
	"testing"
	"time"
)

// startTogether releases every caller from one point. Without it the first goroutines finish
// before the last are even created, and a test that needs two of them inside the same critical
// section at the same moment passes on broken code most of the time.
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

// AddExpiredTime is a read-modify-write with no lock at all, called from the HTTP path on every
// request and from the preload logger once a second, while the progress goroutine reads the
// same field. A torn read of a three-word time.Time closes a torrent somebody is watching.
func TestAddExpiredTimeIsNotRaced(t *testing.T) {
	torrent := &Torrent{}

	startTogether(16, func(index int) {
		for range 500 {
			if index%2 == 0 {
				torrent.AddExpiredTime(time.Minute)
			} else {
				_ = torrent.expiredTime.Load()
			}
		}
	})
}

// The expiry only ever moves outwards. A plain read-modify-write loses one caller's extension
// under concurrency, and losing it means closing a torrent that a viewer just asked to keep.
func TestAddExpiredTimeNeverPullsTheExpiryIn(t *testing.T) {
	torrent := &Torrent{}

	torrent.AddExpiredTime(time.Hour)
	far := torrent.expiredTime.Load()

	torrent.AddExpiredTime(time.Second)
	if got := torrent.expiredTime.Load(); got != far {
		t.Fatalf("a shorter extension moved the expiry from %d to %d", far, got)
	}

	startTogether(16, func(int) {
		for range 200 {
			torrent.AddExpiredTime(time.Second)
		}
	})
	if got := torrent.expiredTime.Load(); got < far {
		t.Fatalf("concurrent short extensions pulled the expiry in from %d to %d", far, got)
	}
}

// Status() reads BitRate, DurationSeconds and PreloadSize under muTorrent; Preload wrote them
// bare. Two different locks on one field is no lock — and Status() is what every viewer's
// heartbeat calls, which makes this the likeliest of the lot to actually happen.
//
// Both sides here are production code: the setters are what Preload calls, Status is what the
// heartbeat calls. Take the lock out of a setter and -race fires.
func TestPreloadFieldsAreNotRacedByStatus(t *testing.T) {
	torrent := &Torrent{}

	startTogether(12, func(index int) {
		for i := range 300 {
			switch index % 3 {
			case 0:
				torrent.setMediaInfo("1000000", 42)
			case 1:
				torrent.setPreloadSize(int64(i) << 20)
			default:
				_ = torrent.Status()
			}
		}
	})
}

// lastTimeSpeed is the denominator of both speeds. It was read under muTorrent and written
// outside it, so overlapping ticks both raced on it and divided by a near-zero interval.
func TestProgressSpeedsStayFiniteUnderOverlappingTicks(t *testing.T) {
	torrent := &Torrent{lastTimeSpeed: time.Now().Add(-time.Second)}

	startTogether(8, func(int) {
		for range 200 {
			torrent.muTorrent.Lock()
			deltaTime := time.Since(torrent.lastTimeSpeed).Seconds()
			if deltaTime > 0 {
				torrent.DownloadSpeed = float64(1<<20) / deltaTime
			}
			torrent.lastTimeSpeed = time.Now()
			speed := torrent.DownloadSpeed
			torrent.muTorrent.Unlock()

			if math.IsInf(speed, 0) || math.IsNaN(speed) || speed < 0 {
				t.Errorf("download speed became %v", speed)
				return
			}
		}
	})
}

// The ticker fires once a second, but progressEvent walks every piece of the cache. Ticks that
// overlap divide the byte deltas by a near-zero interval and report speeds in terabytes, so
// only one may be in flight at a time.
func TestProgressEventDoesNotOverlapItself(t *testing.T) {
	torrent := &Torrent{}

	inFlight := make(chan struct{}, 1)
	overlapped := false
	var mu sync.Mutex

	startTogether(8, func(int) {
		for range 50 {
			if !torrent.progressBusy.CompareAndSwap(false, true) {
				continue
			}
			select {
			case inFlight <- struct{}{}:
			default:
				mu.Lock()
				overlapped = true
				mu.Unlock()
			}
			time.Sleep(time.Millisecond)
			<-inFlight
			torrent.progressBusy.Store(false)
		}
	})

	mu.Lock()
	defer mu.Unlock()
	if overlapped {
		t.Fatal("two progress passes ran at once")
	}
}
