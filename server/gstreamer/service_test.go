//go:build gst

package gstreamer

import (
	"context"
	"errors"
	"strconv"
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

func newTrackedTask(token string, lastActive time.Time) (*Task, *trackingRunner) {
	runner := &trackingRunner{}
	return &Task{
		Token:           token,
		Hash:            "hash",
		FileID:          "1",
		LastSentSegment: -1,
		lastActive:      lastActive,
		runner:          runner,
	}, runner
}

// Two clients on one torrent used to contend for its single task slot. Now they hold
// separate sessions, and neither can reach the other's pipeline.
func TestSessionsOfDifferentClientsDoNotCollide(t *testing.T) {
	first, firstRunner := newTrackedTask(sessionToken("c:one", "hash", "1", 0), time.Now().UTC())
	second, secondRunner := newTrackedTask(sessionToken("c:two", "hash", "1", 0), time.Now().UTC())

	service := &Service{
		conf: Config{}.normalized(),
		tasks: map[string]*Task{
			first.Token:  first,
			second.Token: second,
		},
	}

	if first.Token == second.Token {
		t.Fatal("two clients on one file collapsed into one session")
	}
	if service.session(first.Token) != first || service.session(second.Token) != second {
		t.Fatal("a session resolved to the wrong task")
	}
	if firstRunner.disposed.Load() || secondRunner.disposed.Load() {
		t.Fatal("holding two sessions disposed one of them")
	}
}

// A player re-requests master.m3u8 after any error. That must land on the session it
// already has, not mint a second pipeline for the same viewer.
func TestGetOrAddReturnsTheExistingSession(t *testing.T) {
	existing, runner := newTrackedTask(sessionToken("c:one", "hash", "1", 0), time.Now().UTC())

	service := &Service{
		conf:  Config{}.normalized(),
		tasks: map[string]*Task{existing.Token: existing},
	}

	task, err := service.GetOrAdd(context.Background(), "c:one", "hash", "1", 0)
	if err != nil {
		t.Fatal(err)
	}
	if task != existing {
		t.Fatal("a repeated master request created a second task")
	}
	if len(service.tasks) != 1 || runner.disposed.Load() {
		t.Fatal("the existing session did not survive its own repeat request")
	}
}

func TestGetOrAddStopsOnCancelledContext(t *testing.T) {
	service := &Service{
		conf:  Config{}.normalized(),
		tasks: make(map[string]*Task),
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := service.GetOrAdd(ctx, "c:test", "hash", "2", 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("GetOrAdd error=%v, want context.Canceled", err)
	}
}

// At the limit, a session that is somebody's running playback must not be evicted to
// make room — refusing the newcomer is a retry, evicting is a picture that stops.
func TestAdmitSessionRefusesWhenEveryHeldSessionIsLive(t *testing.T) {
	now := time.Now().UTC()
	live, _ := newTrackedTask("live", now)
	service := &Service{
		conf:  Config{MaxTasks: 1}.normalized(),
		tasks: map[string]*Task{live.Token: live},
	}

	service.mu.Lock()
	err := service.admitSessionLocked()
	service.mu.Unlock()
	if !errors.Is(err, ErrTooManySessions) {
		t.Fatalf("admitSessionLocked error=%v, want ErrTooManySessions", err)
	}

	// A session nobody has asked anything of for a while is fair game again.
	live.activeMu.Lock()
	live.lastActive = now.Add(-2 * sessionActiveGrace)
	live.activeMu.Unlock()

	service.mu.Lock()
	err = service.admitSessionLocked()
	service.mu.Unlock()
	if err != nil {
		t.Fatalf("admitSessionLocked error=%v, want a stale session to be evictable", err)
	}
}

func TestAdmitSessionIsUnlimitedByDefault(t *testing.T) {
	service := &Service{conf: Config{}.normalized(), tasks: make(map[string]*Task)}
	for i := range 32 {
		task, _ := newTrackedTask(strconv.Itoa(i), time.Now().UTC())
		service.tasks[task.Token] = task
	}

	service.mu.Lock()
	err := service.admitSessionLocked()
	service.mu.Unlock()
	if err != nil {
		t.Fatalf("admitSessionLocked error=%v, want no limit when MaxTasks is 0", err)
	}
}

// Playlists handed out by an older build carry no session. Resolving them is only safe
// while the answer is unambiguous.
func TestSoleSessionForHashOnlyResolvesWhenUnambiguous(t *testing.T) {
	only, _ := newTrackedTask("only", time.Now().UTC())
	service := &Service{
		conf:  Config{}.normalized(),
		tasks: map[string]*Task{only.Token: only},
	}

	if service.soleSessionForHash("hash") != only {
		t.Fatal("a single session on the torrent should resolve")
	}
	if service.soleSessionForHash("other") != nil {
		t.Fatal("a session of a different torrent must not resolve")
	}

	second, _ := newTrackedTask("second", time.Now().UTC())
	service.tasks[second.Token] = second
	if service.soleSessionForHash("hash") != nil {
		t.Fatal("an ambiguous legacy URL must 404 so the player refetches master.m3u8")
	}
}

// Callers ask by hash because they are removing the torrent, not one viewer's stream.
func TestTryRemoveDropsEverySessionOfTheTorrent(t *testing.T) {
	first, firstRunner := newTrackedTask("first", time.Now().UTC())
	second, secondRunner := newTrackedTask("second", time.Now().UTC())
	other, otherRunner := newTrackedTask("other", time.Now().UTC())
	other.Hash = "other"

	service := &Service{
		conf: Config{}.normalized(),
		tasks: map[string]*Task{
			first.Token:  first,
			second.Token: second,
			other.Token:  other,
		},
	}

	if !service.TryRemove("hash") {
		t.Fatal("TryRemove reported nothing to remove")
	}
	if !firstRunner.disposed.Load() || !secondRunner.disposed.Load() {
		t.Fatal("a session of the removed torrent survived")
	}
	if otherRunner.disposed.Load() || service.tasks[other.Token] != other {
		t.Fatal("a session of another torrent was removed")
	}
	if service.TryRemove("hash") {
		t.Fatal("TryRemove reported a second removal of the same torrent")
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
			oldTask.Token: oldTask,
			newTask.Token: newTask,
		},
	}

	service.mu.Lock()
	evicted := service.evictTasksForLimitLocked(newTask.Token)
	service.mu.Unlock()
	disposeTasks(evicted)

	if _, ok := service.tasks[oldTask.Token]; ok {
		t.Fatal("old task was not evicted")
	}
	if service.tasks[newTask.Token] != newTask {
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
			oldTask.Token: oldTask,
			midTask.Token: midTask,
			newTask.Token: newTask,
		},
	}

	service.mu.Lock()
	evicted := service.evictTasksForLimitLocked(newTask.Token)
	service.mu.Unlock()
	disposeTasks(evicted)

	if _, ok := service.tasks[oldTask.Token]; ok {
		t.Fatal("oldest task was not evicted")
	}
	if service.tasks[midTask.Token] != midTask {
		t.Fatal("newer task was evicted instead of the oldest one")
	}
	if service.tasks[newTask.Token] != newTask {
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
		tasks: map[string]*Task{task.Token: task},
	}

	if service.tryRemoveExpectedInactive(task.Token, task, now.Add(-time.Minute)) {
		t.Fatal("active task was removed")
	}
	if service.tasks[task.Token] != task || task.IsDisposed() || runner.disposed.Load() {
		t.Fatal("active task changed during cleanup recheck")
	}

	task.activeMu.Lock()
	task.lastActive = now.Add(-2 * time.Minute)
	task.activeMu.Unlock()
	if !service.tryRemoveExpectedInactive(task.Token, task, now.Add(-time.Minute)) {
		t.Fatal("inactive task was not removed")
	}
	if !task.IsDisposed() || !runner.disposed.Load() {
		t.Fatal("removed task was not disposed")
	}
}

func TestServiceDisposeRejectsFurtherWork(t *testing.T) {
	task, runner := newTrackedTask("task", time.Now().UTC())
	service := &Service{
		tasks:       map[string]*Task{task.Token: task},
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
	if got := service.session(task.Token); got != nil {
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

// Going ahead after a timeout is right for one caller and wrong for several: they reach the
// timeout together and start together, so the serialisation vanishes under the very load it
// exists for. A few ungated slots come free immediately; past those the wait is longer, and
// that stagger is the point.
func TestTorrentGateDegradesInStages(t *testing.T) {
	service := &Service{conf: Config{}.normalized()}

	held, err := service.acquireTorrent(context.Background(), "hash")
	if err != nil {
		t.Fatal(err)
	}
	defer held()

	const wait = 40 * time.Millisecond
	releases := make([]func(), 0, maxUngatedTorrentOps)
	for i := range maxUngatedTorrentOps {
		start := time.Now()
		release := service.acquireTorrentWithin("hash", wait)
		if elapsed := time.Since(start); elapsed > 3*wait {
			t.Fatalf("ungated slot %d took %v, want it granted after one wait", i, elapsed)
		}
		releases = append(releases, release)
	}

	// Everything is taken, so this one waits both stages before going ahead regardless.
	start := time.Now()
	service.acquireTorrentWithin("hash", wait)()
	if elapsed := time.Since(start); elapsed < 2*wait {
		t.Fatalf("past the ungated slots the wait was %v, want at least both stages %v", elapsed, 2*wait)
	}

	for _, release := range releases {
		release()
	}
}
