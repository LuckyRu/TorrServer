//go:build gst

package gstreamer

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type trackingRunner struct {
	disposed atomic.Bool
	frozen   atomic.Bool
}

func (r *trackingRunner) EnsureInit(context.Context, int, int) error {
	return nil
}

func (r *trackingRunner) GetSegment(context.Context, int, int) (Segment, error) {
	return Segment{}, nil
}

func (r *trackingRunner) Seek(float64) bool {
	return true
}

func (r *trackingRunner) Frozen() {
	r.frozen.Store(true)
}

func (r *trackingRunner) Dispose() {
	r.disposed.Store(true)
}

func (r *trackingRunner) IsFrozen() bool {
	return r.frozen.Load()
}

func newTrackedTask(id string, lastActive time.Time) (*Task, *trackingRunner) {
	runner := &trackingRunner{}
	return &Task{
		ID:              id,
		FileID:          "1",
		LastSentSegment: -1,
		lastActive:      lastActive,
		runner:          runner,
	}, runner
}

// A second client asking for a different file of the same torrent must not be able to
// tear down a task that was itself just installed and is still serving: that is the
// swap fight that used to spin GetOrAdd forever. It has to give up, and give up fast.
func TestGetOrAddDefendsFreshlyInstalledTaskFromSwap(t *testing.T) {
	now := time.Now().UTC()
	existing, runner := newTrackedTask("hash", now)
	existing.CreatedAt = now

	service := &Service{
		conf:  Config{}.normalized(),
		tasks: map[string]*Task{"hash": existing},
	}

	start := time.Now()
	_, err := service.GetOrAdd(context.Background(), "c:test", "hash", "2", 0)
	elapsed := time.Since(start)

	if !errors.Is(err, ErrTaskBusy) {
		t.Fatalf("GetOrAdd error=%v, want ErrTaskBusy", err)
	}
	if runner.disposed.Load() || existing.IsDisposed() {
		t.Fatal("defended task was disposed anyway")
	}
	if service.tasks["hash"] != existing {
		t.Fatal("defended task was replaced in the slot")
	}
	// Bounded: attempts and backoff, not an unbounded spin.
	if elapsed > 5*time.Second {
		t.Fatalf("GetOrAdd took %v, want it bounded by the retry budget", elapsed)
	}
}

func TestGetOrAddStopsOnCancelledContext(t *testing.T) {
	now := time.Now().UTC()
	existing, _ := newTrackedTask("hash", now)
	existing.CreatedAt = now

	service := &Service{
		conf:  Config{}.normalized(),
		tasks: map[string]*Task{"hash": existing},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := service.GetOrAdd(ctx, "c:test", "hash", "2", 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("GetOrAdd error=%v, want context.Canceled", err)
	}
}

// The defence must be narrow: only a task that is both fresh and still being served.
// Anything else — including one the viewer has actually been watching — stays evictable,
// so switching episodes after watching for a while is not delayed.
func TestSwapDefendedOnlyCoversFreshAndActiveTasks(t *testing.T) {
	now := time.Now().UTC()

	settled, _ := newTrackedTask("settled", now)
	settled.CreatedAt = now.Add(-time.Hour)

	idle, _ := newTrackedTask("idle", now.Add(-time.Hour))
	idle.CreatedAt = now

	fresh, _ := newTrackedTask("fresh", now)
	fresh.CreatedAt = now

	disposed, _ := newTrackedTask("disposed", now)
	disposed.CreatedAt = now
	disposed.Dispose()

	for _, test := range []struct {
		name string
		task *Task
		want bool
	}{
		{"nil", nil, false},
		{"watched for a while", settled, false},
		{"fresh but idle", idle, false},
		{"disposed", disposed, false},
		{"fresh and active", fresh, true},
	} {
		if got := swapDefended(test.task); got != test.want {
			t.Fatalf("swapDefended(%s)=%v, want %v", test.name, got, test.want)
		}
	}
}

// Probing, cue reading and pipeline startup each open their own TorrServer reader, and the
// torrent's connection budget is split across open readers — so they have to take turns.
func TestAcquireTorrentSerialisesPerHash(t *testing.T) {
	service := &Service{conf: Config{}.normalized()}

	release, err := service.acquireTorrent(context.Background(), "hash")
	if err != nil {
		t.Fatal(err)
	}

	// A different torrent is unaffected.
	otherRelease, err := service.acquireTorrent(context.Background(), "other")
	if err != nil {
		t.Fatal(err)
	}
	otherRelease()

	// The same torrent waits, and gives up with the caller's context.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := service.acquireTorrent(ctx, "hash"); !errors.Is(err, context.Canceled) {
		t.Fatalf("second acquire error=%v, want context.Canceled", err)
	}

	// Callers without a context proceed anyway rather than stall forever.
	start := time.Now()
	fallback := service.acquireTorrentWithin("hash", 30*time.Millisecond)
	if elapsed := time.Since(start); elapsed < 25*time.Millisecond {
		t.Fatalf("acquireTorrentWithin returned after %v, want it to wait for the timeout", elapsed)
	}
	fallback()

	release()
	again, err := service.acquireTorrent(context.Background(), "hash")
	if err != nil {
		t.Fatal(err)
	}
	again()
}

func TestAcquireTorrentIgnoresEmptyHash(t *testing.T) {
	service := &Service{conf: Config{}.normalized()}
	for range 3 {
		release, err := service.acquireTorrent(context.Background(), "")
		if err != nil {
			t.Fatal(err)
		}
		release()
	}
}

func TestTaskCallKeySeparatesFilesAndAudio(t *testing.T) {
	keys := map[string]struct{}{}
	for _, key := range []string{
		taskCallKey("hash", "1", 0),
		taskCallKey("hash", "2", 0),
		taskCallKey("hash", "1", 1),
		taskCallKey("other", "1", 0),
	} {
		if _, clash := keys[key]; clash {
			t.Fatalf("taskCallKey collision on %q", key)
		}
		keys[key] = struct{}{}
	}
}

// A missing cue index is usually the torrent not having buffered it yet, so it must not
// be remembered for the full hour a real timeline is.
func TestCueCacheExpiresMissingTimelineSooner(t *testing.T) {
	service := &Service{cueCache: make(map[string]cueCacheEntry)}

	service.setCachedCue("present", &CueTimeline{})
	service.setCachedCue("absent", nil)

	present := service.cueCache["present"].expiresAt
	absent := service.cueCache["absent"].expiresAt
	if !absent.Before(present) {
		t.Fatalf("absent cue expires at %v, want sooner than %v", absent, present)
	}

	if cue, ok := service.getCachedCue("present"); !ok || cue == nil {
		t.Fatal("cached cue timeline was not returned")
	}
	if cue, ok := service.getCachedCue("absent"); !ok || cue != nil {
		t.Fatal("cached absence should be a hit carrying a nil timeline")
	}

	service.cleanupCueCache(time.Now().UTC().Add(2 * cueCacheTTL))
	if len(service.cueCache) != 0 {
		t.Fatalf("cue cache still holds %d entries after expiry sweep", len(service.cueCache))
	}
}

func TestTaskLimitOneEvictsExistingTaskForProtectedNewTask(t *testing.T) {
	now := time.Now().UTC()
	oldTask, oldRunner := newTrackedTask("old", now.Add(-time.Minute))
	newTask, newRunner := newTrackedTask("new", now)

	service := &Service{
		conf: Config{MaxTasks: 1}.normalized(),
		tasks: map[string]*Task{
			oldTask.ID: oldTask,
			newTask.ID: newTask,
		},
	}

	service.mu.Lock()
	evicted := service.evictTasksForLimitLocked(newTask.ID)
	service.mu.Unlock()
	disposeTasks(evicted)

	if _, ok := service.tasks[oldTask.ID]; ok {
		t.Fatal("old task was not evicted")
	}
	if service.tasks[newTask.ID] != newTask {
		t.Fatal("protected new task was evicted")
	}
	if !oldTask.IsDisposed() || !oldRunner.disposed.Load() {
		t.Fatal("old task was not disposed")
	}
	if newTask.IsDisposed() || newRunner.disposed.Load() {
		t.Fatal("protected new task was disposed")
	}
}

func TestTaskLimitEvictsOldestTask(t *testing.T) {
	now := time.Now().UTC()
	oldTask, oldRunner := newTrackedTask("old", now.Add(-3*time.Minute))
	midTask, midRunner := newTrackedTask("mid", now.Add(-time.Minute))
	newTask, newRunner := newTrackedTask("new", now)

	service := &Service{
		conf: Config{MaxTasks: 2}.normalized(),
		tasks: map[string]*Task{
			oldTask.ID: oldTask,
			midTask.ID: midTask,
			newTask.ID: newTask,
		},
	}

	service.mu.Lock()
	evicted := service.evictTasksForLimitLocked(newTask.ID)
	service.mu.Unlock()
	disposeTasks(evicted)

	if _, ok := service.tasks[oldTask.ID]; ok {
		t.Fatal("oldest task was not evicted")
	}
	if service.tasks[midTask.ID] != midTask {
		t.Fatal("newer task was evicted instead of the oldest one")
	}
	if service.tasks[newTask.ID] != newTask {
		t.Fatal("protected new task was evicted")
	}
	if !oldTask.IsDisposed() || !oldRunner.disposed.Load() {
		t.Fatal("oldest task was not disposed")
	}
	if midTask.IsDisposed() || midRunner.disposed.Load() {
		t.Fatal("newer task was disposed")
	}
	if newTask.IsDisposed() || newRunner.disposed.Load() {
		t.Fatal("protected new task was disposed")
	}
}

func TestRemoveInactiveTaskRechecksLastActive(t *testing.T) {
	now := time.Now().UTC()
	task, runner := newTrackedTask("task", now)
	service := &Service{
		tasks: map[string]*Task{task.ID: task},
	}

	if service.tryRemoveExpectedInactive(task.ID, task, now.Add(-time.Minute)) {
		t.Fatal("active task was removed")
	}
	if service.tasks[task.ID] != task || task.IsDisposed() || runner.disposed.Load() {
		t.Fatal("active task changed during cleanup recheck")
	}

	task.activeMu.Lock()
	task.lastActive = now.Add(-2 * time.Minute)
	task.activeMu.Unlock()
	if !service.tryRemoveExpectedInactive(task.ID, task, now.Add(-time.Minute)) {
		t.Fatal("inactive task was not removed")
	}
	if !task.IsDisposed() || !runner.disposed.Load() {
		t.Fatal("removed task was not disposed")
	}
}

func TestServiceDisposeRejectsFurtherWork(t *testing.T) {
	task, runner := newTrackedTask("task", time.Now().UTC())
	service := &Service{
		tasks:       map[string]*Task{task.ID: task},
		probeCache:  make(map[string]probeCacheEntry),
		stopCleanup: make(chan struct{}),
	}

	service.Dispose()
	service.Dispose()

	if !service.disposed.Load() {
		t.Fatal("service was not marked disposed")
	}
	if !task.IsDisposed() || !runner.disposed.Load() {
		t.Fatal("service task was not disposed")
	}
	if got := service.Get(task.ID); got != nil {
		t.Fatal("disposed service returned a task")
	}
	if _, err := service.GetOrAdd(context.Background(), "c:test", "hash", "1", 0); !errors.Is(err, ErrServiceClosed) {
		t.Fatalf("GetOrAdd error=%v, want ErrServiceClosed", err)
	}
	if _, err := service.Probe("hash", "1"); !errors.Is(err, ErrServiceClosed) {
		t.Fatalf("Probe error=%v, want ErrServiceClosed", err)
	}
}

func TestCachedProbeIsRevalidatedAfterConfigChange(t *testing.T) {
	service := &Service{
		conf:       Config{TranscodeAVI: true}.normalized(),
		tasks:      make(map[string]*Task),
		probeCache: make(map[string]probeCacheEntry),
	}
	service.setCachedProbe("hash", "1", ProbeInfo{
		Container: "AVI",
		Tracks: []TrackInfo{
			{Type: "video", CapsName: "video/x-h264"},
		},
	})

	service.updateConfig(Config{})
	if _, err := service.Probe("hash", "1"); err == nil || !strings.Contains(err.Error(), "AVI requires TranscodeAVI") {
		t.Fatalf("Probe error=%v, want AVI requires TranscodeAVI", err)
	}
}
