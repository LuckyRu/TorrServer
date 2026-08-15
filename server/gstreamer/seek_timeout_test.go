//go:build gst && ((windows && (amd64 || arm64)) || (linux && (amd64 || arm64)) || (darwin && (amd64 || arm64)))

package gstreamer

import (
	"strings"
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
	stateChecks := 0
	seekWaitOver := false

	api.gstElementGetState = func(element uintptr, state unsafe.Pointer, pending unsafe.Pointer, timeout uint64) int32 {
		stateChecks++
		seekWaitOver = stateChecks >= 2
		return getState(element, state, pending, timeout)
	}
	// Exactly once: a bus that keeps handing out the same EOS would spin any later reader.
	eosDelivered := false
	api.gstBusTimedPopFiltered = func(b uintptr, timeout uint64, filter int32) uintptr {
		if seekWaitOver && !eosDelivered && filter&gstMessageEOS != 0 {
			eosDelivered = true
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
