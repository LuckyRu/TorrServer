//go:build gst

package gstreamer

import (
	"context"
	"errors"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type pipelineRunner interface {
	EnsureInit(ctx context.Context, audio int, startIndex int) error
	GetSegment(ctx context.Context, index int, audio int) (Segment, error)
	Seek(seconds float64) bool
	Frozen()
	Dispose()
	IsFrozen() bool
}

type Task struct {
	// Token identifies the session — one client watching one file — and is what the
	// service keys tasks by. Hash is kept alongside it because the torrent layer and the
	// logs still think in torrents.
	Token    string
	Hash     string
	ClientID string

	FileID    string
	Audio     int
	SourceURL string
	Probe     ProbeInfo
	Cue       *CueTimeline
	Config    Config

	CreatedAt time.Time

	// AcquireTorrent keeps pipeline startup off the torrent while probing or cue reading
	// is using it; nil in tests, where there is no torrent to contend for.
	AcquireTorrent func() func()

	LastSentSegment int

	initMu  sync.RWMutex
	initMP4 []byte
	variant *HLSVariantInfo

	activeMu   sync.RWMutex
	lastActive time.Time

	gateOnce sync.Once
	gate     chan struct{}
	runner   pipelineRunner

	subtitleMu     sync.RWMutex
	subtitleStores map[int]*subtitleStore

	failureMu   sync.Mutex
	failedKey   string
	failedAt    time.Time
	failedError error

	disposed atomic.Bool
}

func NewTask(token string, hash string, client string, fileID string, audio int, sourceURL string, probe ProbeInfo, cue *CueTimeline, conf Config) (*Task, error) {
	now := time.Now().UTC()
	task := &Task{
		Token:           token,
		Hash:            hash,
		ClientID:        client,
		FileID:          fileID,
		Audio:           audio,
		SourceURL:       sourceURL,
		Probe:           probe,
		Cue:             cue,
		Config:          conf.normalized(),
		CreatedAt:       now,
		LastSentSegment: -1,
		lastActive:      now,
	}

	runner, err := newPipelineRunner(task, audio)
	if err != nil {
		return nil, err
	}
	task.runner = runner
	return task, nil
}

// gateChan lazily builds the task gate so a zero-value Task stays usable in tests.
func (t *Task) gateChan() chan struct{} {
	t.gateOnce.Do(func() {
		t.gate = make(chan struct{}, 1)
	})
	return t.gate
}

// lock acquires the task gate and gives up as soon as the request is cancelled.
//
// A plain mutex would make a cancelled request wait its turn and then run a full seek
// nobody is waiting for, so a scrubbing player could keep the task busy for as long as
// it kept scrubbing.
func (t *Task) lock(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	gate := t.gateChan()
	select {
	case gate <- struct{}{}:
		return nil
	default:
	}

	select {
	case gate <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// lockBlocking is for callers without a request context: shutdown and cleanup.
func (t *Task) lockBlocking() {
	t.gateChan() <- struct{}{}
}

// tryLock acquires the gate only if it is free, for callers that would rather skip the
// task than wait behind an in-flight segment.
func (t *Task) tryLock() bool {
	select {
	case t.gateChan() <- struct{}{}:
		return true
	default:
		return false
	}
}

func (t *Task) unlock() {
	<-t.gateChan()
}

func (t *Task) acquireTorrent() func() {
	if t == nil || t.AcquireTorrent == nil {
		return func() {}
	}
	if release := t.AcquireTorrent(); release != nil {
		return release
	}
	return func() {}
}

func (t *Task) UpdateLastActive() {
	t.activeMu.Lock()
	t.lastActive = time.Now().UTC()
	t.activeMu.Unlock()
}

func (t *Task) LastActive() time.Time {
	t.activeMu.RLock()
	defer t.activeMu.RUnlock()
	return t.lastActive
}

func (t *Task) WithInitMP4(consume func([]byte) error) error {
	if consume == nil {
		return errors.New("nil init mp4 consumer")
	}

	t.initMu.RLock()
	defer t.initMu.RUnlock()

	if len(t.initMP4) == 0 {
		return ErrSegmentNotReady
	}
	return consume(t.initMP4)
}

func (t *Task) hasInitMP4() bool {
	t.initMu.RLock()
	defer t.initMu.RUnlock()
	return len(t.initMP4) > 0
}

// videoRemuxChain — единственное определение «этот кодек можно скопировать»: ветка элементов для
// passthrough или "", если копировать нечем. videoIsTranscoded и createPipelineArgs читают её же,
// поэтому «не транскодируем» и «есть чем скопировать» не могут разойтись.
//
// Раньше это были два независимых switch, и они разошлись: неизвестный кодек считался
// копируемым, а ветка для него не писалась — пайплайн собирался без видеодорожки.
func videoRemuxChain(probe ProbeInfo) string {
	switch {
	case probe.IsH264():
		return "mq.src_0 ! h264parse config-interval=0 ! h264timestamper name=video_timestamper ! video/x-h264,stream-format=avc,alignment=au ! mux.video_0 "
	case probe.IsH265():
		return "mq.src_0 ! h265parse config-interval=0 ! h265timestamper name=video_timestamper ! video/x-h265,stream-format=hvc1,alignment=au ! mux.video_0 "
	case probe.IsAV1():
		return "mq.src_0 ! av1parse ! video/x-av1,stream-format=obu-stream,alignment=tu ! mux.video_0 "
	case probe.IsVP9():
		return "mq.src_0 ! vp9parse ! video/x-vp9,alignment=frame ! mux.video_0 "
	default:
		return ""
	}
}

// Живёт здесь, а не рядом с построением пайплайна: решение «транскодируем ли видео» нужно и
// логике задачи, а файл пайплайна собирается только под конкретные платформы.
//
// Правило: что не умеем ремуксировать — транскодируем. Флаги TranscodeX выбирают между копией и
// перекодированием только там, где копия вообще возможна; VP8 и всё, чего нет в videoRemuxChain
// (MPEG-4 ASP, MPEG-2, VC-1, WMV3, MJPEG), идут через декодер безусловно.
func videoIsTranscoded(conf Config, probe ProbeInfo) bool {
	if videoRemuxChain(probe) == "" {
		return true
	}
	if conf.HDRToSDR && probe.Video() != nil && probe.Video().IsHDRVideo() {
		return true
	}
	if conf.TranscodeAVI && probe.IsAVIContainer() {
		return true
	}
	switch {
	case probe.IsH264():
		return conf.TranscodeH264
	case probe.IsH265():
		return conf.TranscodeH265
	case probe.IsAV1():
		return conf.TranscodeAV1
	case probe.IsVP9():
		return conf.TranscodeVP9
	default:
		return true
	}
}

// seekCanBeAccurate отвечает на вопрос «можно ли отдать кадр ровно с запрошенной позиции»,
// а не «есть ли у нас индекс keyframe'ов» — это разные вещи, и раньше они были слеплены в одну.
//
// Точная перемотка возможна там, где поток и так декодируется покадрово: транскод декодирует
// всё, а энкодер ставит свой keyframe на границе сегмента (key-int-max), поэтому доехать до
// точного кадра ничего не стоит сверх уже выполняемой работы.
//
// При passthrough кадры копируются как есть, и начать поток можно только с keyframe исходника —
// значит нужен его индекс. Для Matroska он читается в CueTimeline; для остальных контейнеров
// индекса пока нет, и запрашивать точность бессмысленно: демиксер всё равно отдаст keyframe.
func (t *Task) seekCanBeAccurate() bool {
	if t.Cue != nil {
		return true
	}
	return videoIsTranscoded(t.Config, t.Probe)
}

func (t *Task) segmentStartNS(index int) uint64 {
	if cue, ok := t.Cue.Segment(index); ok {
		return cue.StartNS
	}
	if index <= 0 {
		return 0
	}
	return uint64(index) * uint64(max(t.Config.SegmentSeconds, 1)) * 1_000_000_000
}

func (t *Task) startIndexForSeconds(seconds int) int {
	if seconds <= 0 {
		return 0
	}
	segmentSeconds := max(t.Config.SegmentSeconds, 1)
	if t.Cue != nil {
		targetNS := uint64(seconds) * 1_000_000_000
		for i, segment := range t.Cue.Segments {
			if targetNS < segment.EndNS {
				return i
			}
		}
		return len(t.Cue.Segments)
	}
	duration := t.Probe.DurationSeconds()
	count := 0
	if duration > 0 {
		count = 1 + (duration-1)/segmentSeconds
	}
	return startSegmentIndex(seconds, segmentSeconds, count)
}

func (t *Task) setInitMP4(data []byte) {
	variant := readMP4InitInfo(data)
	if variant != nil {
		if video := t.Probe.Video(); video != nil {
			if variant.Width <= 0 {
				variant.Width = video.Width
			}
			if variant.Height <= 0 {
				variant.Height = video.Height
			}
			if video.FrameRateNum > 0 && video.FrameRateDen > 0 {
				variant.FrameRate = float64(video.FrameRateNum) / float64(video.FrameRateDen)
			}
			if variant.VideoRange == "" {
				variant.VideoRange = strings.ToUpper(video.VideoTransfer)
			}
			if t.Config.HDRToSDR && video.IsHDRVideo() {
				variant.VideoRange = "SDR"
			}
		}
	}

	t.initMu.Lock()
	t.initMP4 = data
	t.variant = variant
	t.initMu.Unlock()
}

func (t *Task) clearInitMP4() {
	t.initMu.Lock()
	t.initMP4 = nil
	t.variant = nil
	t.initMu.Unlock()
}

func (t *Task) hlsVariant() (HLSVariantInfo, bool) {
	t.initMu.RLock()
	defer t.initMu.RUnlock()
	if t.variant == nil {
		return HLSVariantInfo{}, false
	}
	return *t.variant, true
}

func (t *Task) EnsureInit(ctx context.Context, audio int, startIndex int) error {
	if err := t.lock(ctx); err != nil {
		return err
	}
	defer t.unlock()

	if startIndex < 0 {
		startIndex = 0
	}
	if err := t.validateSegmentIndex(startIndex); err != nil {
		return err
	}
	if t.hasInitMP4() && (startIndex == 0 || t.LastSentSegment != -1) {
		return nil
	}
	if t.runner == nil {
		return ErrTaskNotFound
	}

	err := t.runner.EnsureInit(ctx, audio, startIndex)
	if err == nil && startIndex > 0 && t.LastSentSegment == -1 {
		t.LastSentSegment = startIndex - 1
	}
	return err
}

// WithSegment runs consume while the task lock is held, so consume borrows the mp4
// reader arena and must not block. Anything that writes to the network wants
// LeaseSegment instead.
func (t *Task) WithSegment(ctx context.Context, index int, audio int, consume func(Segment) error) error {
	if consume == nil {
		return errors.New("nil segment consumer")
	}

	if err := t.lock(ctx); err != nil {
		return err
	}
	defer t.unlock()

	seg, err := t.segmentLocked(ctx, index, audio)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return consume(seg)
}

// LeaseSegment returns an owned copy of the segment, taken while the lock is held.
//
// The caller writes it out after the lock is gone, so a player that stops reading its
// response can no longer stall every other request on this task.
func (t *Task) LeaseSegment(ctx context.Context, index int, audio int) (*SegmentLease, error) {
	if err := t.recentSegmentFailure(index, audio); err != nil {
		return nil, err
	}
	if err := t.lock(ctx); err != nil {
		return nil, err
	}
	defer t.unlock()

	// Re-check under the gate: the request ahead of us may have just failed this segment.
	if err := t.recentSegmentFailure(index, audio); err != nil {
		return nil, err
	}

	seg, err := t.segmentLocked(ctx, index, audio)
	t.noteSegmentOutcome(index, audio, err)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if seg.Empty() {
		return nil, ErrSegmentNotReady
	}
	return leaseSegment(seg), nil
}

func (t *Task) segmentLocked(ctx context.Context, index int, audio int) (Segment, error) {
	if t.runner == nil {
		return Segment{}, ErrTaskNotFound
	}
	if err := t.validateSegmentIndex(index); err != nil {
		return Segment{}, err
	}

	if t.runner.IsFrozen() {
		if err := t.seekToSegmentLocked(ctx, index); err != nil {
			return Segment{}, err
		}
	} else if t.LastSentSegment == -1 && index > 0 {
		if err := t.seekToSegmentLocked(ctx, index); err != nil {
			return Segment{}, err
		}
	} else if t.LastSentSegment != -1 && t.LastSentSegment != index {
		if index != t.LastSentSegment+1 {
			conf := t.Config.normalized()
			seekRequired := true
			if t.Cue == nil {
				diff := index - t.LastSentSegment

				if diff > 0 && diff <= maxSegmentCatchupSeconds/conf.SegmentSeconds {
					for i := 0; i < diff-1; i++ {
						if ctx.Err() != nil {
							return Segment{}, ctx.Err()
						}

						t.LastSentSegment++
						if _, err := t.runner.GetSegment(ctx, t.LastSentSegment, audio); err != nil {
							t.LastSentSegment--
							return Segment{}, err
						}
					}
					seekRequired = false
				}
			}
			if seekRequired {
				if err := t.seekToSegmentLocked(ctx, index); err != nil {
					return Segment{}, err
				}
			}
		}
	}

	seg, err := t.runner.GetSegment(ctx, index, audio)
	if err != nil {
		return Segment{}, err
	}

	t.LastSentSegment = index
	return seg, nil
}

// seekToSegmentLocked checks ctx first: a seek is the expensive half of a segment
// request, and there is no point running one for a request that is already gone.
func (t *Task) seekToSegmentLocked(ctx context.Context, index int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if cue, ok := t.Cue.Segment(index); ok {
		if !t.runner.Seek(float64(cue.StartNS) / 1_000_000_000) {
			return ErrSegmentNotReady
		}
		return nil
	}
	if t.Cue != nil {
		return ErrEndOfStreamExhausted
	}
	seconds := float64(index) * float64(t.Config.normalized().SegmentSeconds)
	if !t.runner.Seek(seconds) {
		return ErrSegmentNotReady
	}
	return nil
}

func (t *Task) validateSegmentIndex(index int) error {
	if index < 0 {
		return ErrInvalidIdentifier
	}
	if t.Cue != nil {
		if index >= len(t.Cue.Segments) {
			return ErrEndOfStreamExhausted
		}
		return nil
	}

	segmentSeconds := int64(t.Config.normalized().SegmentSeconds)
	if segmentSeconds <= 0 || segmentSeconds > math.MaxInt64/int64(time.Second) {
		return ErrInvalidIdentifier
	}
	segmentDurationNS := segmentSeconds * int64(time.Second)
	if int64(index) > math.MaxInt64/segmentDurationNS {
		return ErrInvalidIdentifier
	}
	if t.Probe.DurationNS > 0 {
		count := 1 + (t.Probe.DurationNS-1)/segmentDurationNS
		if int64(index) >= count {
			return ErrEndOfStreamExhausted
		}
	}
	return nil
}

const maxSegmentCatchupSeconds = 60

// A failing segment fails the same way if asked again immediately, and each attempt costs
// a seek and a fresh reader on the torrent. Players retry hard — four requests inside two
// seconds shows up in the logs — so replay the last error for a moment instead of tearing
// the pipeline down again for each one.
const segmentFailureDebounce = 1500 * time.Millisecond

func segmentFailureKey(index int, audio int) string {
	return strconv.Itoa(index) + ":" + strconv.Itoa(audio)
}

func (t *Task) recentSegmentFailure(index int, audio int) error {
	t.failureMu.Lock()
	defer t.failureMu.Unlock()

	if t.failedError == nil || t.failedKey != segmentFailureKey(index, audio) {
		return nil
	}
	if time.Since(t.failedAt) >= segmentFailureDebounce {
		t.failedError = nil
		return nil
	}
	return t.failedError
}

func (t *Task) noteSegmentOutcome(index int, audio int, err error) {
	// Cancellation says nothing about the segment: the next request may well succeed.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return
	}

	t.failureMu.Lock()
	defer t.failureMu.Unlock()

	if err == nil {
		t.failedError = nil
		return
	}
	t.failedKey = segmentFailureKey(index, audio)
	t.failedAt = time.Now()
	t.failedError = err
}

func (t *Task) Frozen() {
	t.lockBlocking()
	defer t.unlock()
	t.freezeLocked()
}

func (t *Task) FreezeIfInactive(cutoff time.Time) bool {
	t.lockBlocking()
	defer t.unlock()

	t.activeMu.Lock()
	defer t.activeMu.Unlock()
	if !t.lastActive.Before(cutoff) {
		return false
	}

	return t.freezeLocked()
}

func (t *Task) freezeLocked() bool {
	if t.disposed.Load() || t.runner == nil || t.runner.IsFrozen() {
		return false
	}

	t.runner.Frozen()
	t.clearInitMP4()
	return true
}

func (t *Task) Dispose() {
	if !t.disposed.CompareAndSwap(false, true) {
		return
	}

	t.lockBlocking()
	defer t.unlock()
	if t.runner != nil {
		t.runner.Dispose()
		t.runner = nil
	}
	t.clearInitMP4()
}

func (t *Task) IsDisposed() bool {
	return t.disposed.Load()
}

func (t *Task) IsFrozen() bool {
	t.lockBlocking()
	defer t.unlock()

	if t.disposed.Load() || t.runner == nil {
		return false
	}
	return t.runner.IsFrozen()
}
