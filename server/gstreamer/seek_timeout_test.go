//go:build gst && ((windows && (amd64 || arm64)) || (linux && (amd64 || arm64)) || (darwin && (amd64 || arm64)))

package gstreamer

import (
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
)

// A seek into a part of the file nothing has downloaded does not fail outright — it settles
// into EOS, and by then the pipeline state is perfectly healthy. Nothing else catches that:
// the seek used to be declared good, the bus watch restarted, and the EOS it saw a moment
// later was reported as a broken stream to a viewer who had only pressed fast-forward.
func TestReusePipelineTreatsSettledEOSAsASeekFailure(t *testing.T) {
	previous := gstRuntime
	t.Cleanup(func() {
		gstRuntime = previous
	})

	runner, _ := newReusePipelineTestRunner(t, 1, int64(16*time.Second), 1, false)
	defer func() {
		runner.stopPipeline()
		runner.releaseTransientState()
	}()

	// The EOS only becomes visible once the seek wait is over, which is exactly the window
	// the old code walked through without looking.
	api := gstRuntime
	bus := api.gstBusTimedPopFiltered
	getState := api.gstElementGetState
	// State is checked twice: once pausing before the seek, once validating after the wait.
	// Arming on the second is what puts the EOS in the window the old code walked through.
	// The bus watcher reads these from its own goroutine, so plain int/bool here would be a
	// race in the harness itself — and a race reported against the harness is how a package
	// ends up excluded from -race.
	var stateChecks atomic.Int32
	var seekWaitOver atomic.Bool

	api.gstElementGetState = func(element uintptr, state unsafe.Pointer, pending unsafe.Pointer, timeout uint64) int32 {
		seekWaitOver.Store(stateChecks.Add(1) >= 2)
		return getState(element, state, pending, timeout)
	}
	// Exactly once: a bus that keeps handing out the same EOS would spin any later reader.
	var eosDelivered atomic.Bool
	api.gstBusTimedPopFiltered = func(b uintptr, timeout uint64, filter int32) uintptr {
		if seekWaitOver.Load() && filter&gstMessageEOS != 0 && eosDelivered.CompareAndSwap(false, true) {
			return 9
		}
		return bus(b, timeout, filter)
	}

	if _, err := runner.reusePipeline(12, false, time.Millisecond); err == nil ||
		!strings.Contains(err.Error(), "EOS") {
		t.Fatalf("a seek that settled into EOS must fail the seek, got %v", err)
	}
}

// The seek wait is the same wait as a preroll: both sit on the torrent fetching pieces.
// Five seconds is what made a fast-forward into undownloaded video look like a dead stream.
func TestSeekWaitsAsLongAsAPreroll(t *testing.T) {
	if pipelinePrerollTimeout <= pipelineStateTimeout {
		t.Fatalf("preroll budget %v must exceed the local-file budget %v",
			pipelinePrerollTimeout, pipelineStateTimeout)
	}
}

// startPipeline is the path taken after the pipeline died, so it starts from nothing at the
// coldest possible position — yet its post-seek wait used the local-file budget while
// reusePipeline's used the swarm one. A viewer whose pipeline broke mid-film therefore burnt
// a whole attempt on a position that only needed a few more seconds of downloading, and only
// the caller's retry saved the playback.
func TestStartPipelineWaitsOutAnAsyncSeekLikeAPreroll(t *testing.T) {
	previous := gstRuntime
	previousPreroll := pipelinePrerollTimeout
	t.Cleanup(func() {
		gstRuntime = previous
		pipelinePrerollTimeout = previousPreroll
	})
	pipelinePrerollTimeout = 10 * time.Second

	// ASYNC after the seek means "the pieces have not arrived yet", and it is the only
	// state a cold position can report. Settling on the third look stands in for a torrent
	// that needed a moment longer than the five-second budget.
	var seekSent atomic.Bool
	var looksAfterSeek atomic.Int32
	gstRuntime = &gstAPI{
		gstParseLaunch:  func(string, unsafe.Pointer) uintptr { return 1 },
		gstBinGetByName: func(_ uintptr, name string) uintptr { return map[string]uintptr{"mq": 4}[name] + 2 },
		gstPipelineGetBus: func(uintptr) uintptr {
			return 3
		},
		gstElementSetState: func(uintptr, int32) int32 { return gstStateChangeSuccess },
		gstElementGetState: func(uintptr, unsafe.Pointer, unsafe.Pointer, uint64) int32 {
			if !seekSent.Load() {
				return gstStateChangeSuccess
			}
			if looksAfterSeek.Add(1) < 3 {
				return gstStateChangeAsync
			}
			return gstStateChangeSuccess
		},
		gstElementGetStaticPad: func(uintptr, string) uintptr { return 5 },
		gstEventNewSeek: func(float64, int32, int32, int32, int64, int32, int64) uintptr {
			return 6
		},
		gstPadSendEvent: func(uintptr, uintptr) int32 {
			seekSent.Store(true)
			return 1
		},
		gstElementQueryPosition: func(_ uintptr, _ int32, cur unsafe.Pointer) int32 {
			*(*int64)(cur) = int64(453 * time.Second)
			return 1
		},
		gstBusTimedPopFiltered: func(_ uintptr, _ uint64, filter int32) uintptr {
			// ASYNC_DONE never arrives: the state poll is the only thing that can tell a
			// slow seek from a dead one, which is exactly the case being covered.
			return 0
		},
		gstObjectUnref:     func(uintptr) {},
		gstMiniObjectUnref: func(uintptr) {},
	}

	runner := &gstRunner{task: &Task{Config: Config{}.normalized()}}
	actual, err := runner.startPipeline(453.161)
	if err != nil {
		t.Fatalf("a seek that only needed more downloading was reported as a failure: %v", err)
	}
	defer runner.stopPipeline()

	if actual != 453 {
		t.Fatalf("actual=%v, want the queried position 453", actual)
	}
	if got := looksAfterSeek.Load(); got < 3 {
		t.Fatalf("gave up after %d looks at the post-seek state", got)
	}
}
