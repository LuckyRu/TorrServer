//go:build gst

package gstreamer

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"server/settings"
	"server/torr"
	torrstate "server/torr/state"

	"golang.org/x/sync/singleflight"
)

var (
	ErrBadSource               = errors.New("bad gstreamer source")
	ErrUnsupportedContainer    = errors.New("unsupported container")
	ErrUnsupportedHDRTransfer  = errors.New("HDR tone mapping requires a PQ or HLG base layer")
	ErrProbeUnavailable        = errors.New("gst-discoverer returned no stream info")
	ErrPipelineUnavailable     = errors.New("gstreamer runtime is unavailable")
	ErrSegmentNotReady         = errors.New("segment is not ready")
	ErrTaskNotFound            = errors.New("gstreamer task not found")
	ErrServiceClosed           = errors.New("gstreamer service is closed")
	ErrInvalidIdentifier       = errors.New("invalid gstreamer task id")
	ErrEarlyEndOfStream        = errors.New("gstreamer reached EOS before the expected end")
	ErrEndOfStreamExhausted    = errors.New("gstreamer end of stream is exhausted")
	ErrTaskBusy                = errors.New("gstreamer task slot is busy")
	ErrTruncatedMP4Fragment    = errors.New("truncated mp4 fragment at end of stream")
	ErrUndecodableEOSRemainder = errors.New("undecodable mp4 eos remainder")
)

type Service struct {
	conf Config

	mu    sync.RWMutex
	tasks map[string]*Task

	probeMu    sync.Mutex
	probeCache map[string]probeCacheEntry
	probeCalls singleflight.Group
	taskCalls  singleflight.Group

	cueMu    sync.Mutex
	cueCache map[string]cueCacheEntry
	cueCalls singleflight.Group

	cleanupRunning atomic.Bool
	disposed       atomic.Bool
	stopCleanup    chan struct{}
}

const (
	probeCacheTTL = time.Hour

	// A hash owns exactly one task slot, so two clients wanting different files of the
	// same torrent contend for it. Bound the contention instead of letting them evict
	// each other indefinitely.
	taskSwapAttempts = 3
	taskSwapBackoff  = 150 * time.Millisecond
	taskSwapGrace    = 2 * time.Second

	cueCacheTTL         = probeCacheTTL
	cueNegativeCacheTTL = time.Minute
)

type probeCacheEntry struct {
	probe     ProbeInfo
	expiresAt time.Time
}

type cueCacheEntry struct {
	cue       *CueTimeline
	expiresAt time.Time
}

func (s *Service) currentConfig() Config {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.conf
}

func (s *Service) updateConfig(conf Config) {
	if s.disposed.Load() {
		return
	}
	conf = conf.normalized()

	s.mu.Lock()
	s.conf = conf
	evicted := s.evictTasksForLimitLocked("")
	s.mu.Unlock()

	disposeTasks(evicted)
}

func NewService(conf Config) *Service {
	conf = conf.normalized()

	service := &Service{
		conf:        conf,
		tasks:       make(map[string]*Task),
		probeCache:  make(map[string]probeCacheEntry),
		cueCache:    make(map[string]cueCacheEntry),
		stopCleanup: make(chan struct{}),
	}
	go service.cleanupLoop()
	return service
}

func (s *Service) GetOrAdd(ctx context.Context, hash string, fileID string, audio int) (*Task, error) {
	if hash == "" || fileID == "" {
		return nil, ErrBadSource
	}

	for attempt := 0; attempt < taskSwapAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if s.disposed.Load() {
			return nil, ErrServiceClosed
		}

		// Keyed by the full request, not by hash: collapsing two different files of one
		// torrent into a single call hands one of the callers a task it did not ask for,
		// which is what used to send it around this loop again.
		value, err, _ := s.taskCalls.Do(taskCallKey(hash, fileID, audio), func() (any, error) {
			return s.getOrAdd(ctx, hash, fileID, audio)
		})
		if err != nil {
			if !errors.Is(err, ErrTaskBusy) {
				return nil, err
			}
		} else if task, ok := value.(*Task); !ok {
			return nil, errors.New("gstreamer task creation returned an invalid result")
		} else if taskMatchesRequest(task, hash, fileID, audio) {
			return task, nil
		}

		// Either the slot was defended against us or someone swapped it out between
		// creation and return. Both are contention, so back off rather than spin.
		if attempt < taskSwapAttempts-1 {
			if waitErr := sleepContext(ctx, taskSwapBackoff); waitErr != nil {
				return nil, waitErr
			}
		}
	}

	return nil, ErrTaskBusy
}

func (s *Service) getOrAdd(ctx context.Context, hash string, fileID string, audio int) (*Task, error) {
	if s.disposed.Load() {
		return nil, ErrServiceClosed
	}
	conf := s.currentConfig()
	sourceURL := sourceURL(conf, hash, fileID)
	id := hash

	s.mu.RLock()
	task := s.tasks[id]
	if task != nil && task.FileID == fileID && task.Audio == audio && !task.IsDisposed() {
		task.UpdateLastActive()
		s.mu.RUnlock()
		return task, nil
	}
	blocked := swapDefended(task)
	s.mu.RUnlock()

	// Bail before the expensive part: probing and reading a cue timeline for a task we
	// will not be allowed to install is pure waste.
	if blocked {
		return nil, ErrTaskBusy
	}

	probe, err := s.Probe(hash, fileID)
	if err != nil {
		return nil, err
	}
	cue := s.cueTimeline(ctx, conf, hash, fileID, sourceURL, probe)

	task, err = NewTask(id, fileID, audio, sourceURL, probe, cue, conf)
	if err != nil {
		return nil, err
	}

	var replaced *Task
	var evicted []*Task

	s.mu.Lock()
	if s.disposed.Load() {
		s.mu.Unlock()
		task.Dispose()
		return nil, ErrServiceClosed
	}
	existing := s.tasks[id]
	if existing != nil &&
		existing.FileID == fileID &&
		existing.Audio == audio &&
		!existing.IsDisposed() {
		existing.UpdateLastActive()
		s.mu.Unlock()

		task.Dispose()
		return existing, nil
	}
	if swapDefended(existing) {
		s.mu.Unlock()
		task.Dispose()
		return nil, ErrTaskBusy
	}

	replaced = existing
	s.tasks[id] = task
	evicted = s.evictTasksForLimitLocked(id)
	s.mu.Unlock()

	if replaced != nil {
		replaced.Dispose()
	}
	disposeTasks(evicted)

	return task, nil
}

// swapDefended reports whether evicting task right now would most likely be one half of
// a swap fight rather than a viewer switching episodes.
//
// The signal is that the task is both freshly created and still being served: a task
// somebody has actually been watching has an old CreatedAt and is evicted immediately,
// so ordinary episode switches stay instant. Two clients trading the slot back and forth
// only ever see fresh tasks, and get throttled to one swap per grace period.
func swapDefended(task *Task) bool {
	if task == nil || task.IsDisposed() {
		return false
	}
	now := time.Now().UTC()
	return now.Sub(task.CreatedAt) < taskSwapGrace && now.Sub(task.LastActive()) < taskSwapGrace
}

func taskCallKey(hash string, fileID string, audio int) string {
	return hash + "\x00" + fileID + "\x00" + strconv.Itoa(audio)
}

func sleepContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func taskMatchesRequest(task *Task, hash string, fileID string, audio int) bool {
	return task != nil && !task.IsDisposed() && task.ID == hash && task.FileID == fileID && task.Audio == audio
}

func shouldUseCueTimeline(conf Config, probe ProbeInfo) bool {
	if !probe.IsMatroskaContainer() {
		return false
	}
	switch {
	case probe.IsH264():
		return !conf.TranscodeH264
	case probe.IsH265():
		return !conf.TranscodeH265
	case probe.IsAV1():
		return !conf.TranscodeAV1
	case probe.IsVP9():
		return !conf.TranscodeVP9
	default:
		return false
	}
}

func (s *Service) evictTasksForLimitLocked(protectedID string) []*Task {
	limit := s.conf.normalized().MaxTasks
	if limit <= 0 || len(s.tasks) <= limit {
		return nil
	}

	evicted := make([]*Task, 0, len(s.tasks)-limit)
	for len(s.tasks) > limit {
		id, task := oldestEvictableTask(s.tasks, protectedID)
		if id == "" {
			break
		}
		delete(s.tasks, id)
		if task != nil {
			evicted = append(evicted, task)
		}
	}
	return evicted
}

func oldestEvictableTask(tasks map[string]*Task, protectedID string) (string, *Task) {
	var oldestID string
	var oldestTask *Task
	var oldestActive time.Time
	oldestDisposed := false

	for id, task := range tasks {
		if id == protectedID {
			continue
		}
		if task == nil {
			return id, nil
		}

		disposed := task.IsDisposed()
		lastActive := task.LastActive()
		if oldestID == "" || (disposed && !oldestDisposed) || (disposed == oldestDisposed && lastActive.Before(oldestActive)) {
			oldestID = id
			oldestTask = task
			oldestActive = lastActive
			oldestDisposed = disposed
		}
	}
	return oldestID, oldestTask
}

func disposeTasks(tasks []*Task) {
	for _, task := range tasks {
		if task != nil {
			task.Dispose()
		}
	}
}

func (s *Service) Probe(hash string, fileID string) (ProbeInfo, error) {
	if hash == "" || fileID == "" {
		return ProbeInfo{}, ErrBadSource
	}
	if s.disposed.Load() {
		return ProbeInfo{}, ErrServiceClosed
	}

	if probe, ok, err := s.cachedProbe(hash, fileID); ok {
		return probe, err
	}

	key := probeCacheKey(hash, fileID)
	value, err, _ := s.probeCalls.Do(key, func() (any, error) {
		if s.disposed.Load() {
			return ProbeInfo{}, ErrServiceClosed
		}
		if cached, found, err := s.cachedProbe(hash, fileID); found {
			return cached, err
		}
		conf := s.currentConfig()
		result, err := probeSource(sourceURL(conf, hash, fileID), conf)
		if err != nil {
			return ProbeInfo{}, err
		}
		result = refreshProbeFileSize(result, hash, fileID)
		if err := validateProbe(result, conf); err != nil {
			return ProbeInfo{}, err
		}
		if s.disposed.Load() {
			return ProbeInfo{}, ErrServiceClosed
		}
		s.setCachedProbe(hash, fileID, result)
		return result, nil
	})
	if err != nil {
		return ProbeInfo{}, err
	}
	if s.disposed.Load() {
		return ProbeInfo{}, ErrServiceClosed
	}
	probe := refreshProbeFileSize(value.(ProbeInfo), hash, fileID)
	if err := validateProbe(probe, s.currentConfig()); err != nil {
		return ProbeInfo{}, err
	}
	s.setCachedProbe(hash, fileID, probe)
	return probe, nil
}

// cueTimeline reads the Matroska cue index, or returns the cached one.
//
// The read costs HTTP range requests against the torrent and used to run uncancellable
// on every task creation, so a client that had already gone away still paid for it.
func (s *Service) cueTimeline(ctx context.Context, conf Config, hash string, fileID string, sourceURL string, probe ProbeInfo) *CueTimeline {
	if !shouldUseCueTimeline(conf, probe) {
		return nil
	}

	key := probeCacheKey(hash, fileID)
	if cue, ok := s.getCachedCue(key); ok {
		return cue
	}

	value, err, _ := s.cueCalls.Do(key, func() (any, error) {
		if cue, ok := s.getCachedCue(key); ok {
			return cue, nil
		}
		cue := readMatroskaCueTimeline(ctx, sourceURL, probe.FileSize, probe.DurationNS)
		if cue == nil && ctx.Err() != nil {
			// Cancelled, not absent: caching this would deny the next caller a cue
			// timeline it could have had.
			return nil, ctx.Err()
		}
		s.setCachedCue(key, cue)
		return cue, nil
	})
	if err != nil {
		return nil
	}

	cue, _ := value.(*CueTimeline)
	return cue
}

func (s *Service) getCachedCue(key string) (*CueTimeline, bool) {
	now := time.Now().UTC()

	s.cueMu.Lock()
	defer s.cueMu.Unlock()

	entry, ok := s.cueCache[key]
	if !ok {
		return nil, false
	}
	if !now.Before(entry.expiresAt) {
		delete(s.cueCache, key)
		return nil, false
	}
	return entry.cue, true
}

func (s *Service) setCachedCue(key string, cue *CueTimeline) {
	if s.disposed.Load() {
		return
	}

	ttl := cueCacheTTL
	if cue == nil {
		// Absence is often transient (the torrent has not buffered the index yet), so
		// retry sooner than a real timeline expires, but not on every request.
		ttl = cueNegativeCacheTTL
	}

	s.cueMu.Lock()
	defer s.cueMu.Unlock()
	if s.disposed.Load() {
		return
	}

	if s.cueCache == nil {
		s.cueCache = make(map[string]cueCacheEntry)
	}
	s.cueCache[key] = cueCacheEntry{cue: cue, expiresAt: time.Now().UTC().Add(ttl)}
}

func (s *Service) cleanupCueCache(now time.Time) {
	s.cueMu.Lock()
	defer s.cueMu.Unlock()

	for key, entry := range s.cueCache {
		if !now.Before(entry.expiresAt) {
			delete(s.cueCache, key)
		}
	}
}

func (s *Service) cachedProbe(hash string, fileID string) (ProbeInfo, bool, error) {
	probe, ok := s.getCachedProbe(hash, fileID)
	if !ok {
		return ProbeInfo{}, false, nil
	}

	probe = refreshProbeFileSize(probe, hash, fileID)
	if err := validateProbe(probe, s.currentConfig()); err != nil {
		return ProbeInfo{}, true, err
	}
	s.setCachedProbe(hash, fileID, probe)
	return probe, true, nil
}

func validateProbe(probe ProbeInfo, conf Config) error {
	if len(probe.Tracks) == 0 || probe.Video() == nil {
		return ErrProbeUnavailable
	}
	if conf.HDRToSDR && probe.Video().IsHDRVideo() && probe.Video().VideoTransfer != "pq" && probe.Video().VideoTransfer != "hlg" {
		return ErrUnsupportedHDRTransfer
	}
	if probe.DemuxerName() == "" {
		name := strings.TrimSpace(probe.Container)
		if name == "" {
			name = "<unknown>"
		}
		return fmt.Errorf("%w: %s", ErrUnsupportedContainer, name)
	}
	if probe.IsAVIContainer() && !conf.TranscodeAVI {
		return errors.New("AVI requires TranscodeAVI")
	}
	return nil
}

func torrentFileSize(hash string, fileID string) (size int64) {
	index, err := strconv.Atoi(fileID)
	if err != nil || index <= 0 {
		return 0
	}

	tor := getTorrentForGStreamer(hash)
	if tor == nil {
		return 0
	}

	if size := torrentStatusFileSize(tor.Status(), index); size > 0 {
		return size
	}
	if tor.Torrent == nil {
		return 0
	}
	if !tor.GotInfo() {
		return 0
	}

	return torrentStatusFileSize(tor.Status(), index)
}

type heartbeatState struct {
	Hash    string                   `json:"Hash"`
	Torrent *torrstate.TorrentStatus `json:"Torrent,omitempty"`
}

func torrentHeartbeatState(hash string) (state any) {
	state = heartbeatState{Hash: hash}

	defer func() {
		if recover() != nil {
			state = heartbeatState{Hash: hash}
		}
	}()

	tor := getTorrentForGStreamer(hash)
	if tor == nil {
		return state
	}

	cacheState := tor.CacheState()
	if cacheState != nil {
		return cacheState
	}

	return heartbeatState{
		Hash:    hash,
		Torrent: tor.Status(),
	}
}

func dropTorrentForGStreamer(hash string) {
	defer func() {
		_ = recover()
	}()

	if hash == "" {
		return
	}
	torr.DropTorrent(hash)
}

func getTorrentForGStreamer(hash string) (tor *torr.Torrent) {
	defer func() {
		if recover() != nil {
			tor = nil
		}
	}()

	if hash == "" {
		return nil
	}
	return torr.GetTorrent(hash)
}

func torrentStatusFileSize(status *torrstate.TorrentStatus, index int) int64 {
	if status == nil {
		return 0
	}
	for _, file := range status.FileStats {
		if file != nil && file.Id == index && file.Length > 0 {
			return file.Length
		}
	}
	return 0
}

func refreshProbeFileSize(probe ProbeInfo, hash string, fileID string) ProbeInfo {
	if size := torrentFileSize(hash, fileID); size > 0 {
		probe.FileSize = size
	}
	return probe
}

func (s *Service) getCachedProbe(hash string, fileID string) (ProbeInfo, bool) {
	key := probeCacheKey(hash, fileID)
	now := time.Now().UTC()

	s.probeMu.Lock()
	defer s.probeMu.Unlock()

	entry, ok := s.probeCache[key]
	if !ok {
		return ProbeInfo{}, false
	}
	if !now.Before(entry.expiresAt) {
		delete(s.probeCache, key)
		return ProbeInfo{}, false
	}
	return cloneProbeInfo(entry.probe), true
}

func (s *Service) setCachedProbe(hash string, fileID string, probe ProbeInfo) {
	if s.disposed.Load() {
		return
	}
	key := probeCacheKey(hash, fileID)

	s.probeMu.Lock()
	defer s.probeMu.Unlock()
	if s.disposed.Load() {
		return
	}

	if s.probeCache == nil {
		s.probeCache = make(map[string]probeCacheEntry)
	}
	s.probeCache[key] = probeCacheEntry{
		probe:     cloneProbeInfo(probe),
		expiresAt: time.Now().UTC().Add(probeCacheTTL),
	}
}

func (s *Service) cleanupProbeCache(now time.Time) {
	s.probeMu.Lock()
	defer s.probeMu.Unlock()

	for key, entry := range s.probeCache {
		if !now.Before(entry.expiresAt) {
			delete(s.probeCache, key)
		}
	}
}

func probeCacheKey(hash string, fileID string) string {
	return hash + "\x00" + fileID
}

func cloneProbeInfo(probe ProbeInfo) ProbeInfo {
	if len(probe.Tracks) == 0 {
		return probe
	}
	probe.Tracks = append([]TrackInfo(nil), probe.Tracks...)
	return probe
}

func (s *Service) Get(id string) *Task {
	if id == "" || s.disposed.Load() {
		return nil
	}

	s.mu.RLock()
	task := s.tasks[id]
	if task == nil || task.IsDisposed() {
		s.mu.RUnlock()
		return nil
	}
	task.UpdateLastActive()
	s.mu.RUnlock()
	return task
}

func (s *Service) TryRemove(id string) bool {
	task, ok := s.detachTask(id, nil)
	if !ok {
		return false
	}

	task.Dispose()
	return true
}

func (s *Service) detachTask(id string, expected *Task) (*Task, bool) {
	if id == "" {
		return nil, false
	}

	s.mu.Lock()
	task := s.tasks[id]
	if task == nil || (expected != nil && task != expected) {
		s.mu.Unlock()
		return nil, false
	}

	delete(s.tasks, id)
	s.mu.Unlock()
	return task, true
}

func (s *Service) tryRemoveExpectedInactive(id string, expected *Task, cutoff time.Time) bool {
	if id == "" || expected == nil {
		return false
	}

	s.mu.Lock()
	task := s.tasks[id]
	if task != expected || !task.LastActive().Before(cutoff) {
		s.mu.Unlock()
		return false
	}
	delete(s.tasks, id)
	s.mu.Unlock()

	task.Dispose()
	return true
}

func (s *Service) Dispose() {
	if !s.disposed.CompareAndSwap(false, true) {
		return
	}
	if s.stopCleanup != nil {
		close(s.stopCleanup)
	}

	s.mu.Lock()
	tasks := s.tasks
	s.tasks = make(map[string]*Task)
	s.mu.Unlock()

	s.probeMu.Lock()
	s.probeCache = make(map[string]probeCacheEntry)
	s.probeMu.Unlock()

	s.cueMu.Lock()
	s.cueCache = make(map[string]cueCacheEntry)
	s.cueMu.Unlock()

	for _, task := range tasks {
		task.Dispose()
	}
}

func (s *Service) cleanupLoop() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			func() {
				defer func() {
					if recovered := recover(); recovered != nil {
						gstErrorf("inactive cleanup panic: %v", recovered)
					}
				}()
				s.cleanupInactive()
			}()
		case <-s.stopCleanup:
			return
		}
	}
}

func (s *Service) cleanupInactive() {
	if s.disposed.Load() {
		return
	}
	if !s.cleanupRunning.CompareAndSwap(false, true) {
		return
	}
	defer s.cleanupRunning.Store(false)

	now := time.Now().UTC()

	type snapshotEntry struct {
		id   string
		task *Task
	}
	s.mu.RLock()
	snapshot := make([]snapshotEntry, 0, len(s.tasks))
	for id, task := range s.tasks {
		snapshot = append(snapshot, snapshotEntry{id: id, task: task})
	}
	s.mu.RUnlock()

	conf := s.currentConfig()
	inactiveDuration := conf.inactiveDuration()
	removeAfter := inactiveDuration + 20*time.Minute
	freezeCutoff := now.Add(-inactiveDuration)
	removeCutoff := now.Add(-removeAfter)

	for _, entry := range snapshot {
		id, task := entry.id, entry.task
		lastActive := task.LastActive()
		if lastActive.Before(removeCutoff) {
			s.tryRemoveExpectedInactive(id, task, removeCutoff)
			continue
		}
		if lastActive.Before(freezeCutoff) && s.isCurrentTask(id, task) {
			task.FreezeIfInactive(freezeCutoff)
		}
	}

	s.cleanupProbeCache(now)
	s.cleanupCueCache(now)
}

func (s *Service) isCurrentTask(id string, expected *Task) bool {
	s.mu.RLock()
	current := s.tasks[id]
	s.mu.RUnlock()
	return current == expected
}

func sourceURL(conf Config, hash string, fileID string) string {
	if conf.normalized().Source == "play" {
		return playURL(hash, fileID)
	}
	return streamURL(hash, fileID)
}

func streamURL(hash string, fileID string) string {
	return "http://127.0.0.1:" + settings.Port + "/stream/?link=" + url.QueryEscape(hash) + "&index=" + url.QueryEscape(fileID) + "&play"
}

func playURL(hash string, fileID string) string {
	return "http://127.0.0.1:" + settings.Port + "/play/" + url.PathEscape(hash) + "/" + url.PathEscape(fileID)
}
