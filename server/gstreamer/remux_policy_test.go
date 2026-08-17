//go:build gst && ((windows && (amd64 || arm64)) || (linux && (amd64 || arm64)) || (darwin && (amd64 || arm64)))

package gstreamer

import (
	"strings"
	"testing"
)

// Правило: что не умеем ремуксировать — транскодируем.
//
// Caps здесь всегда выводится из строки кодека через codecToCapsName — так же, как это делает
// парсер gst-discoverer. Прошлый тест на неизвестный кодек ставил CapsName руками в значение,
// которое парсер вернуть не может, и потому проверял недостижимую ветку.

// Кодеки без ветки копирования: codecToCapsName возвращает для них "" либо caps, для которого
// нет parser + mux caps. Всё это обязано уходить в декодер.
var codecsWithoutRemux = []string{
	"MPEG-4 Video",
	"Xvid",
	"MPEG-2 Video",
	"VC-1",
	"Windows Media Video 9",
	"Motion JPEG",
	"H.263",
	"Theora",
	"VP8",
}

var codecsWithRemux = []string{"H.264", "HEVC", "AV1", "VP9"}

type containerCase struct {
	name      string
	container string
	capsName  string
	conf      Config
}

var pipelineContainers = []containerCase{
	{name: "Matroska", container: "Matroska", capsName: "video/x-matroska"},
	{name: "MP4", container: "ISO MP4/M4A", capsName: "video/quicktime"},
	{name: "AVI", container: "AVI", capsName: "video/x-msvideo", conf: Config{TranscodeAVI: true}},
}

func probeWithCodec(container containerCase, codec string) ProbeInfo {
	return ProbeInfo{
		Container:         container.container,
		ContainerCapsName: container.capsName,
		Tracks: []TrackInfo{{
			Type:     "video",
			Index:    0,
			Codec:    codec,
			CapsName: codecToCapsName("video", codec),
		}},
	}
}

func runnerForProbe(probe ProbeInfo, conf Config) *gstRunner {
	conf.GSTVersion = 1.28
	return &gstRunner{
		task: &Task{
			SourceURL: "http://127.0.0.1/video",
			Config:    conf.normalized(),
			Probe:     probe,
		},
		audioIndex: -1,
	}
}

func TestCodecsWithoutRemuxChainAreTranscoded(t *testing.T) {
	for _, container := range pipelineContainers {
		for _, codec := range codecsWithoutRemux {
			probe := probeWithCodec(container, codec)
			if chain := videoRemuxChain(probe); chain != "" {
				t.Fatalf("%s/%s: неожиданно есть ветка копирования: %s", container.name, codec, chain)
			}
			if !videoIsTranscoded(container.conf.normalized(), probe) {
				t.Fatalf("%s/%s: копировать нечем, но транскод не включён", container.name, codec)
			}
		}
	}
}

// Свойство, которое ловит исходный дефект напрямую: любой принятый файл обязан дать пайплайн,
// где src-пад мультиочереди связан, а у mux есть видеовход. Раньше неизвестный кодек проходил
// проверку и собирал строку без видеоветки — mq.src_0 не был связан ни с чем.
func TestPipelineAlwaysLinksVideoBranch(t *testing.T) {
	clearGStreamerRuntimeVersion(t)

	for _, container := range pipelineContainers {
		for _, codec := range append(append([]string{}, codecsWithoutRemux...), codecsWithRemux...) {
			args := runnerForProbe(probeWithCodec(container, codec), container.conf).createPipelineArgs()

			for _, want := range []string{"d.video_0 ! mq.sink_0", "mq.src_0", "mux.video_0"} {
				if !strings.Contains(args, want) {
					t.Fatalf("%s/%s: в пайплайне нет %q:\n%s", container.name, codec, want, args)
				}
			}
		}
	}
}

func TestCodecsWithRemuxChainAreCopiedByDefault(t *testing.T) {
	clearGStreamerRuntimeVersion(t)

	for _, codec := range codecsWithRemux {
		container := pipelineContainers[0]
		probe := probeWithCodec(container, codec)

		chain := videoRemuxChain(probe)
		if chain == "" {
			t.Fatalf("%s: ветка копирования потеряна", codec)
		}
		if videoIsTranscoded(Config{}.normalized(), probe) {
			t.Fatalf("%s: копируемый кодек ушёл в транскод без указания в конфиге", codec)
		}

		args := runnerForProbe(probe, Config{}).createPipelineArgs()
		if !strings.Contains(args, chain) {
			t.Fatalf("%s: пайплайн не использует ветку копирования %q:\n%s", codec, chain, args)
		}
		if strings.Contains(args, "decodebin") {
			t.Fatalf("%s: копируемый кодек всё равно декодируется:\n%s", codec, args)
		}
	}
}

func TestForcedTranscodeWinsOverRemuxChain(t *testing.T) {
	clearGStreamerRuntimeVersion(t)

	forced := map[string]Config{
		"H.264": {TranscodeH264: true},
		"HEVC":  {TranscodeH265: true},
		"AV1":   {TranscodeAV1: true},
		"VP9":   {TranscodeVP9: true},
	}
	for codec, conf := range forced {
		probe := probeWithCodec(pipelineContainers[0], codec)
		if !videoIsTranscoded(conf.normalized(), probe) {
			t.Fatalf("%s: флаг конфигурации не включил транскод", codec)
		}
		if args := runnerForProbe(probe, conf).createPipelineArgs(); !strings.Contains(args, "mq.src_0 ! decodebin !") {
			t.Fatalf("%s: пайплайн не декодирует при принудительном транскоде:\n%s", codec, args)
		}
	}
}

// Индекс исходника описывает выходной поток только при копировании. Проверяем именно связь с
// решением о транскоде, а не отдельный список кодеков: раньше это были два независимых switch.
func TestCueTimelineFollowsTranscodeDecision(t *testing.T) {
	matroska := pipelineContainers[0]

	for _, codec := range codecsWithoutRemux {
		if shouldUseCueTimeline(Config{}.normalized(), probeWithCodec(matroska, codec)) {
			t.Fatalf("%s: индекс запрошен для потока, который перекодируется", codec)
		}
	}
	for _, codec := range codecsWithRemux {
		if !shouldUseCueTimeline(Config{}.normalized(), probeWithCodec(matroska, codec)) {
			t.Fatalf("%s: индекс не запрошен для копируемого потока", codec)
		}
	}

	hdr := probeWithCodec(matroska, "HEVC")
	hdr.Tracks[0].VideoTransfer = "pq"
	if shouldUseCueTimeline(Config{HDRToSDR: true}.normalized(), hdr) {
		t.Fatal("HDR→SDR перекодирует поток, но индекс исходника всё равно запрошен")
	}
}

// VP8 копировать нечем: у mp4mux нет для него ветки. Значит транскод безусловен, а TranscodeVP8
// не участвует в решении — настройка существует, но ничего не выбирает.
func TestVP8IsAlwaysTranscoded(t *testing.T) {
	probe := probeWithCodec(pipelineContainers[0], "VP8")

	for _, flag := range []bool{false, true} {
		if !videoIsTranscoded(Config{TranscodeVP8: flag}.normalized(), probe) {
			t.Fatalf("TranscodeVP8=%v: VP8 не перекодируется, хотя ремуксировать его нечем", flag)
		}
	}
}
