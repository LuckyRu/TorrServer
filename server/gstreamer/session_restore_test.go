//go:build gst

package gstreamer

import (
	"context"
	"errors"
	"testing"
	"time"
)

// Сервер убирает сессию сам — по неактивности или вытесняя по лимиту, — а у плеера все ссылки
// несут её токен. Раньше вернувшийся с паузы плеер получал 404 без строки в логе и больше не
// восстанавливался. Токен детерминирован, поэтому сессию поднимаем под тем же токеном.

type builtSession struct {
	token, client, hash, fileID string
	audio                       int
}

// stubSessionTask подменяет конструктор задачи: сессия создаётся без GStreamer, а тест видит,
// с какими параметрами её пересобрали.
func stubSessionTask(t *testing.T) *[]builtSession {
	t.Helper()
	built := &[]builtSession{}
	previous := newSessionTask
	newSessionTask = func(token string, hash string, client string, fileID string, audio int, _ string, _ ProbeInfo, _ *CueTimeline, _ Config) (*Task, error) {
		*built = append(*built, builtSession{token: token, client: client, hash: hash, fileID: fileID, audio: audio})
		return &Task{
			Token: token, Hash: hash, ClientID: client, FileID: fileID, Audio: audio,
			LastSentSegment: -1, lastActive: time.Now().UTC(), runner: &trackingRunner{},
		}, nil
	}
	t.Cleanup(func() { newSessionTask = previous })
	return built
}

// Перекодируемый поток, чтобы восстановление не пошло читать cue-индекс из сети.
func restorableService(conf Config) *Service {
	service := &Service{
		conf:       conf.normalized(),
		tasks:      make(map[string]*Task),
		probeCache: make(map[string]probeCacheEntry),
		cueCache:   make(map[string]cueCacheEntry),
	}
	service.setCachedProbe("hash", "7", ProbeInfo{
		Container: "Matroska",
		Tracks:    []TrackInfo{{Type: "video", Codec: "MPEG-2 Video", CapsName: codecToCapsName("video", "MPEG-2 Video")}},
	})
	return service
}

func sessionFor(client string, audio int, lastActive time.Time) *Task {
	return &Task{
		Token: sessionToken(client, "hash", "7", audio), Hash: "hash", ClientID: client, FileID: "7", Audio: audio,
		LastSentSegment: -1, lastActive: lastActive, runner: &trackingRunner{},
	}
}

func TestSessionRemovedForInactivityIsRestoredUnderTheSameToken(t *testing.T) {
	built := stubSessionTask(t)
	service := restorableService(Config{})
	old := sessionFor("c:tv", 2, time.Now().UTC().Add(-48*time.Hour))
	service.tasks[old.Token] = old

	if !service.tryRemoveExpectedInactive(old.Token, old, time.Now().UTC()) {
		t.Fatal("неактивная сессия не удалена")
	}
	if service.session(old.Token) != nil {
		t.Fatal("удалённая сессия всё ещё находится")
	}

	restored, err := service.restoreExpiredSession(context.Background(), "hash", old.Token)
	if err != nil || restored == nil {
		t.Fatalf("плеер вернулся по старым ссылкам, а сессия не восстановлена: %v", err)
	}
	if restored.Token != old.Token {
		t.Fatalf("сессия поднята под другим токеном: %s, want %s — старые ссылки останутся мёртвыми", restored.Token, old.Token)
	}
	if len(*built) != 1 {
		t.Fatalf("сессия собрана %d раз, want 1", len(*built))
	}
	got := (*built)[0]
	if got.client != "c:tv" || got.fileID != "7" || got.audio != 2 {
		t.Fatalf("сессия пересобрана не с теми параметрами: %+v", got)
	}
	if service.session(old.Token) != restored {
		t.Fatal("восстановленная сессия не зарегистрирована — следующий запрос снова пойдёт в 404")
	}
}

func TestSessionEvictedByTheLimitIsRestoredOnceASlotFrees(t *testing.T) {
	stubSessionTask(t)
	service := restorableService(Config{MaxTasks: 1})
	now := time.Now().UTC()
	paused := sessionFor("c:tv", 0, now.Add(-time.Hour))
	other := sessionFor("c:phone", 0, now.Add(-time.Minute))
	service.tasks[paused.Token] = paused
	service.tasks[other.Token] = other

	service.mu.Lock()
	disposeTasks(service.evictTasksForLimitLocked(other.Token))
	service.mu.Unlock()
	if service.session(paused.Token) != nil {
		t.Fatal("давно неактивная сессия не вытеснена")
	}

	restored, err := service.restoreExpiredSession(context.Background(), "hash", paused.Token)
	if err != nil || restored == nil || restored.Token != paused.Token {
		t.Fatalf("вытесненная сессия не восстановлена под своим токеном: task=%v err=%v", restored, err)
	}
}

// Пока каждый слот занят тем, кто прямо сейчас смотрит, поднимать вытесненную сессию нельзя —
// это остановит чужую картинку. Отказ должен быть повторяемым (503), а надгробие — остаться,
// чтобы повтор после освобождения слота сработал.
func TestRestoreWaitsWhileEverySlotIsPlaying(t *testing.T) {
	stubSessionTask(t)
	service := restorableService(Config{MaxTasks: 1})
	now := time.Now().UTC()
	paused := sessionFor("c:tv", 0, now.Add(-time.Hour))
	watching := sessionFor("c:phone", 0, now)
	service.tasks[paused.Token] = paused
	service.tasks[watching.Token] = watching

	service.mu.Lock()
	disposeTasks(service.evictTasksForLimitLocked(watching.Token))
	service.mu.Unlock()

	restored, err := service.restoreExpiredSession(context.Background(), "hash", paused.Token)
	if restored != nil {
		t.Fatal("вытесненная сессия поднята ценой остановки того, кто смотрит")
	}
	if !errors.Is(err, ErrTooManySessions) {
		t.Fatalf("err=%v, want ErrTooManySessions — иначе плеер получит 404 вместо повторяемого 503", err)
	}
	if service.session(watching.Token) != watching {
		t.Fatal("играющая сессия пострадала от попытки восстановления")
	}

	watching.activeMu.Lock()
	watching.lastActive = now.Add(-time.Minute)
	watching.activeMu.Unlock()
	restored, err = service.restoreExpiredSession(context.Background(), "hash", paused.Token)
	if err != nil || restored == nil || restored.Token != paused.Token {
		t.Fatalf("после освобождения слота повтор не восстановил сессию: task=%v err=%v", restored, err)
	}
}

func TestUnknownTokenIsNotRestored(t *testing.T) {
	built := stubSessionTask(t)
	service := restorableService(Config{})

	if task, _ := service.restoreExpiredSession(context.Background(), "hash", "never-issued"); task != nil {
		t.Fatal("сессия поднята по токену, который сервер не выдавал")
	}
	if len(*built) != 0 {
		t.Fatal("по чужому токену создавалась задача")
	}
}

func TestRestoreRequiresTheSameTorrent(t *testing.T) {
	built := stubSessionTask(t)
	service := restorableService(Config{})
	old := sessionFor("c:tv", 0, time.Now().UTC().Add(-48*time.Hour))
	service.tasks[old.Token] = old
	service.tryRemoveExpectedInactive(old.Token, old, time.Now().UTC())

	if task, _ := service.restoreExpiredSession(context.Background(), "other-hash", old.Token); task != nil {
		t.Fatal("сессия поднята для другого торрента")
	}
	if len(*built) != 0 {
		t.Fatal("для другого торрента создавалась задача")
	}
}

// Явное удаление — намерение, а не потеря: такую сессию поднимать нельзя.
func TestExplicitlyRemovedSessionIsNotRestored(t *testing.T) {
	built := stubSessionTask(t)
	service := restorableService(Config{})
	old := sessionFor("c:tv", 0, time.Now().UTC())
	service.tasks[old.Token] = old

	if !service.TryRemove("hash") {
		t.Fatal("явное удаление не сработало")
	}
	if task, _ := service.restoreExpiredSession(context.Background(), "hash", old.Token); task != nil {
		t.Fatal("явно удалённая сессия поднялась снова")
	}
	if len(*built) != 0 {
		t.Fatal("после явного удаления создавалась задача")
	}
}

// Плеер после паузы шлёт плейлист, init и сегмент почти одновременно — все должны восстановиться.
func TestParallelRequestsAllRestoreTheSameSession(t *testing.T) {
	stubSessionTask(t)
	service := restorableService(Config{})
	old := sessionFor("c:tv", 0, time.Now().UTC().Add(-48*time.Hour))
	service.tasks[old.Token] = old
	service.tryRemoveExpectedInactive(old.Token, old, time.Now().UTC())

	first, _ := service.restoreExpiredSession(context.Background(), "hash", old.Token)
	second, _ := service.resolveSession(context.Background(), "hash", old.Token)
	if first == nil || second == nil {
		t.Fatalf("параллельный запрос не восстановился: first=%v second=%v", first, second)
	}
	if first != second {
		t.Fatal("два запроса одной паузы подняли две разные сессии")
	}
}

// Замороженная сессия раньше удалялась через 25 минут простоя; пауза на обед её убивала.
func TestFrozenSessionOutlivesTheOldRemovalWindow(t *testing.T) {
	service := restorableService(Config{})
	paused := sessionFor("c:tv", 0, time.Now().UTC().Add(-2*time.Hour))
	service.tasks[paused.Token] = paused

	service.cleanupInactive()

	if service.session(paused.Token) != paused {
		t.Fatal("сессия на двухчасовой паузе удалена")
	}
	if !paused.runner.IsFrozen() {
		t.Fatal("сессия на паузе не заморожена — ресурсы не освобождены")
	}
}
