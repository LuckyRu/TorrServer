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
	ErrTooManySessions         = errors.New("too many gstreamer sessions are already playing")
	ErrTruncatedMP4Fragment    = errors.New("truncated mp4 fragment at end of stream")
	ErrUndecodableEOSRemainder = errors.New("undecodable mp4 eos remainder")
)

type Service struct {
	conf Config

	mu sync.RWMutex
	// Keyed by session token, not by hash: a pipeline belongs to one client watching one
	// file, so two clients on one torrent no longer share, or fight over, a single slot.
	tasks map[string]*Task

	probeMu    sync.Mutex
	probeCache map[string]probeCacheEntry
	probeCalls singleflight.Group
	taskCalls  singleflight.Group

	cueMu    sync.Mutex
	cueCache map[string]cueCacheEntry
	cueCalls singleflight.Group

	gateMu sync.Mutex
	gates  map[string]*torrentGate

	clients clientRegistry

	cleanupRunning atomic.Bool
	disposed       atomic.Bool
	stopCleanup    chan struct{}
}

const (
	probeCacheTTL = time.Hour

	// A session served this recently is somebody's running playback, and evicting it to
	// make room would be visible as a stall. Past the limit with nothing older than this,
	// the honest answer is to refuse the newcomer.
	sessionActiveGrace = 15 * time.Second

	cueCacheTTL         = probeCacheTTL
	cueNegativeCacheTTL = time.Minute

	// How long an operation waits for the torrent gate before going ahead regardless.
	probeGateWait    = 20 * time.Second
	pipelineGateWait = 15 * time.Second
	cueGateWait      = 15 * time.Second
	// cueReadBudget bounds the shared cue read, which no single caller may cancel.
	cueReadBudget = 30 * time.Second
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

// GetOrAdd resolves the caller's session, creating its pipeline if this is the first
// request of that session.
//
// There is no contention to resolve here any more: the session token already separates
// clients, so two of them can never be asking for the same task unless they really are
// the same client asking twice.
func (s *Service) GetOrAdd(ctx context.Context, client string, hash string, fileID string, audio int) (*Task, error) {
	if hash == "" || fileID == "" {
		return nil, ErrBadSource
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.disposed.Load() {
		return nil, ErrServiceClosed
	}

	token := sessionToken(client, hash, fileID, audio)
	value, err, _ := s.taskCalls.Do(token, func() (any, error) {
		return s.getOrAdd(ctx, token, client, hash, fileID, audio)
	})
	if err != nil {
		return nil, err
	}
	task, ok := value.(*Task)
	if !ok || task == nil {
		return nil, errors.New("gstreamer task creation returned an invalid result")
	}
	return task, nil
}

func (s *Service) getOrAdd(ctx context.Context, token string, client string, hash string, fileID string, audio int) (*Task, error) {
	if s.disposed.Load() {
		return nil, ErrServiceClosed
	}
	conf := s.currentConfig()
	sourceURL := sourceURL(conf, hash, fileID)

	if task := s.session(token); task != nil {
		return task, nil
	}

	probe, err := s.Probe(hash, fileID)
	if err != nil {
		return nil, err
	}
	cue := s.cueTimeline(ctx, conf, hash, fileID, sourceURL, probe)

	task, err := NewTask(token, hash, client, fileID, audio, sourceURL, probe, cue, conf)
	if err != nil {
		return nil, err
	}
	task.AcquireTorrent = func() func() { return s.acquireTorrentWithin(hash, pipelineGateWait) }

	s.mu.Lock()
	if s.disposed.Load() {
		s.mu.Unlock()
		task.Dispose()
		return nil, ErrServiceClosed
	}
	// Only a disposed leftover can sit on this token; a live one was returned above.
	replaced := s.tasks[token]
	if replaced != nil && !replaced.IsDisposed() {
		replaced.UpdateLastActive()
		s.mu.Unlock()
		task.Dispose()
		return replaced, nil
	}
	if replaced == nil {
		if err := s.admitSessionLocked(); err != nil {
			s.mu.Unlock()
			task.Dispose()
			return nil, err
		}
	}
	s.tasks[token] = task
	evicted := s.evictTasksForLimitLocked(token)
	s.mu.Unlock()

	if replaced != nil {
		replaced.Dispose()
	}
	disposeTasks(evicted)

	return task, nil
}

// admitSessionLocked refuses a new session when every existing one is somebody's running
// playback and the limit is reached.
//
// Eviction alone would be wrong here: with a task per session rather than per torrent,
// making room means stalling a viewer who is watching right now. Refusing is a 503 the
// player retries; evicting is a picture that stops.
func (s *Service) admitSessionLocked() error {
	limit := s.conf.normalized().MaxTasks
	if limit <= 0 || len(s.tasks) < limit {
		return nil
	}

	cutoff := time.Now().UTC().Add(-sessionActiveGrace)
	for _, task := range s.tasks {
		if task == nil || task.IsDisposed() || task.LastActive().Before(cutoff) {
			return nil
		}
	}
	return ErrTooManySessions
}

// acquireTorrent serialises the operations that each open their own TorrServer reader:
// probing, reading the cue index, starting a pipeline.
//
// TorrServer splits a torrent's connection budget across every open reader
// (ConnectionsLimit / len(readers) in the cache prioritiser) and gives each reader its own
// "fetch this piece now" claim at its own offset. Running these at the same time therefore
// starves whichever one the viewer is actually waiting for, which is how a pipeline ends up
// unable to preroll while the swarm is plainly fast.
//
// The gate is per torrent, not per file, because the budget is shared per torrent.
func (s *Service) acquireTorrent(ctx context.Context, hash string) (func(), error) {
	if hash == "" {
		return func() {}, nil
	}

	gate := s.torrentGate(hash)
	select {
	case gate.turn <- struct{}{}:
		return func() { <-gate.turn }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// maxUngatedTorrentOps bounds how many operations may run on a torrent without waiting
// their turn.
//
// Giving up after a timeout and going ahead anyway is right for one caller and wrong for
// several: they all reach the timeout at about the same moment and start together, so the
// serialisation disappears exactly under the load it exists for. A few ungated slots plus a
// second wait stagger the starts instead of releasing them in a burst.
const maxUngatedTorrentOps = 2

type torrentGate struct {
	turn    chan struct{}
	ungated chan struct{}
}

// acquireTorrentWithin is for callers without a request context. It gives up after the
// timeout and lets the caller proceed anyway: waiting forever behind another torrent
// operation would be a worse failure than the contention it is trying to avoid.
func (s *Service) acquireTorrentWithin(hash string, timeout time.Duration) func() {
	if hash == "" {
		return func() {}
	}

	gate := s.torrentGate(hash)
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case gate.turn <- struct{}{}:
		return func() { <-gate.turn }
	case <-timer.C:
	}

	// The turn did not come. Take one of the few ungated slots, or keep waiting for either
	// for one more round.
	timer.Reset(timeout)
	select {
	case gate.turn <- struct{}{}:
		return func() { <-gate.turn }
	case gate.ungated <- struct{}{}:
		return func() { <-gate.ungated }
	case <-timer.C:
		// Everything is busy and waiting longer would be a worse failure than the
		// contention it is avoiding.
		return func() {}
	}
}

// Gates are kept for the lifetime of the service: one channel per torrent ever played is
// negligible, and reclaiming them safely would need reference counting for no real gain.
func (s *Service) torrentGate(hash string) *torrentGate {
	s.gateMu.Lock()
	defer s.gateMu.Unlock()

	if s.gates == nil {
		s.gates = make(map[string]*torrentGate)
	}
	gate := s.gates[hash]
	if gate == nil {
		gate = &torrentGate{
			turn:    make(chan struct{}, 1),
			ungated: make(chan struct{}, maxUngatedTorrentOps),
		}
		s.gates[hash] = gate
	}
	return gate
}

// Индекс исходника описывает выходной поток только при копировании: под транскодом опорные кадры
// ставит энкодер. Условие выводится из решения о транскоде, а не повторяет его своим switch —
// раньше повтор терял HDR→SDR, где индекс тоже не про выходной поток.
func shouldUseCueTimeline(conf Config, probe ProbeInfo) bool {
	return probe.IsMatroskaContainer() && !videoIsTranscoded(conf, probe)
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
		// gst-discoverer opens its own TorrServer reader; keep it off the torrent while
		// another operation is already reading it. See acquireTorrent.
		release := s.acquireTorrentWithin(hash, probeGateWait)
		result, err := probeSource(sourceURL(conf, hash, fileID), conf)
		release()
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

// readCueTimeline is a seam: the shared cue read is what a test needs to hold still while it
// checks that one caller going away does not fail the others.
var readCueTimeline = readMatroskaCueTimeline

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
		// Deliberately not the caller's context. The work is shared through singleflight, so
		// honouring one caller's cancellation fails everyone who joined it — and a player
		// cancels requests as a matter of course. The budget bounds it instead, and the
		// result is cached, so nothing is wasted if the first caller does go away.
		shared, cancel := context.WithTimeout(context.WithoutCancel(ctx), cueReadBudget)
		defer cancel()

		release := s.acquireTorrentWithin(hash, cueGateWait)
		cue := readCueTimeline(shared, sourceURL, probe.FileSize, probe.DurationNS)
		release()
		if cue == nil && shared.Err() != nil {
			// Timed out, not absent: caching this would deny the next caller a cue
			// timeline it could have had.
			return nil, shared.Err()
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

// Проекция CacheState под индикатор воспроизведения. Имена полей совпадают с heartbeat, чтобы
// клиент читал оба ответа одним кодом.
type playbackPiece struct {
	Size      int64 `json:"Size"`
	Completed bool  `json:"Completed"`
}

type playbackReader struct {
	Start  int `json:"Start"`
	End    int `json:"End"`
	Reader int `json:"Reader"`
}

type playbackState struct {
	Hash          string                `json:"Hash"`
	PiecesLength  int64                 `json:"PiecesLength"`
	PiecesCount   int                   `json:"PiecesCount"`
	Capacity      int64                 `json:"Capacity"`
	Filled        int64                 `json:"Filled"`
	DownloadSpeed float64               `json:"DownloadSpeed"`
	Readers       []playbackReader      `json:"Readers"`
	Pieces        map[int]playbackPiece `json:"Pieces"`
}

// Клиент идёт по кускам от позиции читателя до конца его окна и останавливается на первом
// незавершённом, поэтому куски за пределами окон читателей ему не нужны ни при каком развитии
// событий. Именно они и составляют почти весь объём ответа heartbeat.
func torrentPlaybackState(hash string) (result any) {
	state := playbackState{Hash: hash, Pieces: map[int]playbackPiece{}}
	result = state

	defer func() {
		if recover() != nil {
			result = playbackState{Hash: hash, Pieces: map[int]playbackPiece{}}
		}
	}()

	tor := getTorrentForGStreamer(hash)
	if tor == nil {
		return result
	}

	if status := tor.Status(); status != nil {
		state.DownloadSpeed = status.DownloadSpeed
	}

	cacheState := tor.CacheState()
	if cacheState == nil {
		return state
	}

	state.PiecesLength = cacheState.PiecesLength
	state.PiecesCount = cacheState.PiecesCount
	state.Capacity = cacheState.Capacity
	state.Filled = cacheState.Filled

	for _, reader := range cacheState.Readers {
		if reader == nil {
			continue
		}
		state.Readers = append(state.Readers, playbackReader{
			Start: reader.Start, End: reader.End, Reader: reader.Reader,
		})
		for index := reader.Reader; index <= reader.End; index++ {
			if _, seen := state.Pieces[index]; seen {
				continue
			}
			piece, ok := cacheState.Pieces[index]
			if !ok {
				continue
			}
			state.Pieces[index] = playbackPiece{Size: piece.Size, Completed: piece.Completed}
		}
	}

	return state
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

// session resolves a session token to its task.
func (s *Service) session(token string) *Task {
	if token == "" || s.disposed.Load() {
		return nil
	}

	s.mu.RLock()
	task := s.tasks[token]
	if task == nil || task.IsDisposed() {
		s.mu.RUnlock()
		return nil
	}
	task.UpdateLastActive()
	s.mu.RUnlock()
	return task
}

// soleSessionForHash serves URLs that predate the session segment in the path.
//
// Such URLs only ever come from a playlist a previous build handed out, so resolving them
// is a courtesy, not a contract: with exactly one session on the torrent the answer is
// unambiguous, and with more than one a 404 sends the player back to master.m3u8 for
// links that carry a token.
func (s *Service) soleSessionForHash(hash string) *Task {
	if hash == "" || s.disposed.Load() {
		return nil
	}

	s.mu.RLock()
	var found *Task
	for _, task := range s.tasks {
		if task == nil || task.IsDisposed() || task.Hash != hash {
			continue
		}
		if found != nil {
			s.mu.RUnlock()
			return nil
		}
		found = task
	}
	if found == nil {
		s.mu.RUnlock()
		return nil
	}
	found.UpdateLastActive()
	s.mu.RUnlock()
	return found
}

func (s *Service) hasSessionForHash(hash string) bool {
	if hash == "" || s.disposed.Load() {
		return false
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, task := range s.tasks {
		if task != nil && !task.IsDisposed() && task.Hash == hash {
			return true
		}
	}
	return false
}

// TryRemove drops every session of a torrent: callers ask by hash because they are
// removing the torrent, not one viewer's stream of it.
func (s *Service) TryRemove(hash string) bool {
	if hash == "" {
		return false
	}

	s.mu.Lock()
	var removed []*Task
	for token, task := range s.tasks {
		if task == nil || task.Hash == hash {
			delete(s.tasks, token)
			if task != nil {
				removed = append(removed, task)
			}
		}
	}
	s.mu.Unlock()

	disposeTasks(removed)
	return len(removed) > 0
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
	s.clients.cleanup(now)
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
