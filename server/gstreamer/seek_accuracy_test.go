//go:build gst

package gstreamer

import "testing"

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
