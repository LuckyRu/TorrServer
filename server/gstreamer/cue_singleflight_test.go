//go:build gst

package gstreamer

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func cueTestService(t *testing.T) *Service {
	t.Helper()
	return &Service{
		conf:       Config{}.normalized(),
		tasks:      make(map[string]*Task),
		probeCache: make(map[string]probeCacheEntry),
		cueCache:   make(map[string]cueCacheEntry),
	}
}

func matroskaProbe() ProbeInfo {
	return ProbeInfo{
		Container:  "Matroska",
		FileSize:   1 << 20,
		DurationNS: int64(time.Hour),
		Tracks: []TrackInfo{
			{Type: "video", CapsName: "video/x-h264", Width: 1920, Height: 1080},
		},
	}
}

// A cue read is shared through singleflight, so it runs on whichever caller happened to arrive
// first. Running it on that caller's context makes everyone who joined a hostage to them: a
// player cancels requests as a matter of course, and the viewer who paid the price never asked
// for anything. On the old code the joiner got nil every time.
func TestCueTimelineSurvivesTheFirstCallerCancelling(t *testing.T) {
	previous := readCueTimeline
	t.Cleanup(func() { readCueTimeline = previous })

	service := cueTestService(t)
	probe := matroskaProbe()

	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	var sawCancelledContext atomic.Bool

	readCueTimeline = func(ctx context.Context, _ string, _ int64, durationNS int64) *CueTimeline {
		once.Do(func() {
			close(entered)
			<-release
		})
		if ctx.Err() != nil {
			sawCancelledContext.Store(true)
			return nil
		}
		return &CueTimeline{
			Segments:      []CueSegment{{}},
			MaxDurationNS: uint64(durationNS),
		}
	}

	firstCtx, cancelFirst := context.WithCancel(context.Background())
	go func() {
		service.cueTimeline(firstCtx, service.currentConfig(), "hash", "1", "http://source", probe)
	}()
	<-entered // the shared read is in flight, owned by the first caller

	joined := make(chan *CueTimeline, 1)
	go func() {
		joined <- service.cueTimeline(context.Background(), service.currentConfig(), "hash", "1", "http://source", probe)
	}()
	// Give the second caller time to join the same singleflight call rather than start its own.
	time.Sleep(20 * time.Millisecond)

	cancelFirst()
	close(release)

	select {
	case cue := <-joined:
		if cue == nil {
			t.Fatal("the first caller cancelling failed a caller who had not cancelled anything")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the joined caller never returned")
	}
	if sawCancelledContext.Load() {
		t.Fatal("the shared read ran on a context one caller could cancel")
	}
}

// The budget still has to bound the shared read: detaching it from the caller must not turn it
// into work nothing can stop.
func TestCueTimelineReadIsBounded(t *testing.T) {
	previous := readCueTimeline
	t.Cleanup(func() { readCueTimeline = previous })

	if cueReadBudget <= 0 {
		t.Fatal("the shared cue read has no budget and nothing else can cancel it")
	}

	var deadlineWasSet atomic.Bool
	readCueTimeline = func(ctx context.Context, _ string, _ int64, _ int64) *CueTimeline {
		_, ok := ctx.Deadline()
		deadlineWasSet.Store(ok)
		return nil
	}

	service := cueTestService(t)
	service.cueTimeline(context.Background(), service.currentConfig(), "hash", "1", "http://source", matroskaProbe())

	if !deadlineWasSet.Load() {
		t.Fatal("the shared cue read got a context with no deadline")
	}
}
