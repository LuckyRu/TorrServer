//go:build gst && ((windows && (amd64 || arm64)) || (linux && (amd64 || arm64)) || (darwin && (amd64 || arm64)))

package gstreamer

import (
	"testing"
	"unsafe"
)

// Точность перемотки раньше выводилась из наличия CueTimeline. Индекс строится только для
// Matroska, поэтому на всех остальных контейнерах перемотка молча становилась приблизительной:
// демиксер отдавал ближайший keyframe, и позиция уезжала на секунды вперёд.
//
// Вопрос на самом деле другой: декодируем ли мы поток покадрово. Если да, доехать до точного
// кадра ничего не стоит сверх уже выполняемой работы.
func TestSeekCanBeAccurate(t *testing.T) {
	matroska := ProbeInfo{Container: "Matroska", ContainerCapsName: "video/x-matroska"}
	avi := ProbeInfo{
		Container:         "AVI",
		ContainerCapsName: "video/x-msvideo",
		Tracks:            []TrackInfo{{Type: "video", CapsName: "video/x-h264"}},
	}
	mp4 := ProbeInfo{
		Container:         "ISO MP4",
		ContainerCapsName: "video/quicktime",
		Tracks:            []TrackInfo{{Type: "video", CapsName: "video/x-h264"}},
	}

	tests := []struct {
		name string
		task Task
		want bool
	}{
		{
			name: "matroska с индексом keyframe'ов",
			task: Task{Probe: matroska, Cue: &CueTimeline{}},
			want: true,
		},
		{
			name: "AVI под транскодом: декодируется покадрово, индекс не нужен",
			task: Task{Probe: avi, Config: Config{TranscodeAVI: true}},
			want: true,
		},
		{
			name: "H.264 под транскодом в любом контейнере",
			task: Task{Probe: mp4, Config: Config{TranscodeH264: true}},
			want: true,
		},
		{
			name: "MP4 passthrough: кадры копируются, начать можно только с keyframe исходника",
			task: Task{Probe: mp4},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.task.seekCanBeAccurate(); got != tt.want {
				t.Fatalf("seekCanBeAccurate() = %v, ожидалось %v", got, tt.want)
			}
		})
	}
}

// AVI без включённого транскода остаётся приблизительным: точность просить не у кого, поток
// копируется как есть. Тест фиксирует, что признак идёт именно от транскода, а не от контейнера.
func TestSeekAccuracyFollowsTranscodeNotContainer(t *testing.T) {
	avi := Task{Probe: ProbeInfo{
		Container:         "AVI",
		ContainerCapsName: "video/x-msvideo",
		Tracks:            []TrackInfo{{Type: "video", CapsName: "video/x-h264"}},
	}}

	if avi.seekCanBeAccurate() {
		t.Fatal("passthrough AVI не может отдать кадр с произвольной позиции")
	}

	avi.Config.TranscodeAVI = true
	if !avi.seekCanBeAccurate() {
		t.Fatal("под транскодом AVI обязан перематываться точно")
	}
}

// KEY_UNIT|SNAP_AFTER и ACCURATE — взаимоисключающие требования, и вместе первые два побеждают.
// Пока они стояли всегда, включение ACCURATE не меняло ничего: перемотка всё равно уезжала на
// следующий keyframe исходника.
func TestVideoSeekFlags(t *testing.T) {
	accurate := videoSeekFlags(true)
	if accurate&gstSeekFlagAccurate == 0 {
		t.Fatal("точная перемотка обязана просить ACCURATE")
	}
	if accurate&(gstSeekFlagKeyUnit|gstSeekFlagSnapAfter) != 0 {
		t.Fatalf("ACCURATE не сочетается с KEY_UNIT/SNAP_AFTER, получено %d", accurate)
	}

	approximate := videoSeekFlags(false)
	if approximate&(gstSeekFlagKeyUnit|gstSeekFlagSnapAfter) != gstSeekFlagKeyUnit|gstSeekFlagSnapAfter {
		t.Fatalf("без точности перемотка обязана вставать на keyframe, получено %d", approximate)
	}
	if approximate&gstSeekFlagAccurate != 0 {
		t.Fatalf("ACCURATE без возможности его выполнить не запрашивается, получено %d", approximate)
	}

	if accurate&gstSeekFlagFlush == 0 || approximate&gstSeekFlagFlush == 0 {
		t.Fatal("обе ветки обязаны сбрасывать пайплайн")
	}
}

// Запрос позиции возвращает позицию демиксера, а при точной перемотке он стоит на опорном кадре
// ПЕРЕД целью — кадры между ними снимает clip probe. Отдать её наружу значит соврать назад на
// длину GOP: сегменты начнут строиться раньше запрошенного, и плеер получит уже показанное.
func TestSeekPositionNeverGoesBackWhenAccurate(t *testing.T) {
	runner := &gstRunner{task: &Task{}}

	restore := stubSeekPosition(t, 657.621)
	defer restore()

	if got := runner.seekPositionAfter(1, 660, true); got != 660 {
		t.Fatalf("точная перемотка отдала %v вместо запрошенных 660", got)
	}

	// Без точности демиксер честно встал на keyframe, и это и есть позиция потока.
	if got := runner.seekPositionAfter(1, 660, false); got != 657.621 {
		t.Fatalf("приблизительная перемотка обязана отдавать фактическую позицию, получено %v", got)
	}
}

// Перемотка вперёд от запрошенной точки остаётся как есть: поток действительно начнётся там.
func TestSeekPositionKeepsForwardDrift(t *testing.T) {
	runner := &gstRunner{task: &Task{}}

	restore := stubSeekPosition(t, 663.4)
	defer restore()

	if got := runner.seekPositionAfter(1, 660, true); got != 663.4 {
		t.Fatalf("сдвиг вперёд обязан сохраняться, получено %v", got)
	}
}

// Подменяет запрос позиции у пайплайна: настоящий уходит в GStreamer через purego, а тесту нужно
// зафиксировать ровно ту позицию, которую демиксер отдаёт после точной перемотки.
func stubSeekPosition(t *testing.T, seconds float64) func() {
	t.Helper()
	previous := gstRuntime
	gstRuntime = &gstAPI{
		gstElementQueryPosition: func(_ uintptr, _ int32, cur unsafe.Pointer) int32 {
			*(*int64)(cur) = int64(seconds * 1_000_000_000)
			return 1
		},
	}
	return func() { gstRuntime = previous }
}
