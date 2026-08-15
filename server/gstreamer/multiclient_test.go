//go:build gst

package gstreamer

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

// viewerRunner serves segments whose bytes identify the session that produced them. That is
// the whole point: a viewer receiving another viewer's bytes is the failure this file exists
// to catch, and it is invisible to any check that only looks at status codes.
type viewerRunner struct {
	token    string
	segments atomic.Int64
	seeks    atomic.Int64
}

func (r *viewerRunner) EnsureInit(context.Context, int, int) error { return nil }

func (r *viewerRunner) GetSegment(_ context.Context, index int, _ int) (Segment, error) {
	r.segments.Add(1)
	return Segment{
		Header:   []byte("moof"),
		Payloads: [][]byte{[]byte(fmt.Sprintf("%s:%d", r.token, index))},
	}, nil
}

func (r *viewerRunner) Seek(float64) bool { r.seeks.Add(1); return true }
func (r *viewerRunner) Frozen()           {}
func (r *viewerRunner) Dispose()          {}
func (r *viewerRunner) IsFrozen() bool    { return false }

func multiclientProbe() ProbeInfo {
	return ProbeInfo{
		// Long enough that the segment indices below are inside the media: a request past the
		// known duration is refused on purpose, and that refusal is not what this file tests.
		DurationNS: int64(time.Hour),
		Container:  "Matroska",
		Tracks: []TrackInfo{
			{Type: "video", CapsName: "video/x-h264", Width: 1920, Height: 1080},
			{Type: "audio", Index: 0, CapsName: "audio/mpeg"},
		},
	}
}

// Every viewer of one release: its own client id, its own file, its own pipeline.
type viewer struct {
	client string
	fileID string
	token  string
	runner *viewerRunner
}

func newMulticlientService(t *testing.T, hash string, viewers []*viewer) (*gin.Engine, *Service) {
	t.Helper()
	gin.SetMode(gin.ReleaseMode)

	conf := Config{}.normalized()
	service := &Service{
		conf:       conf,
		tasks:      make(map[string]*Task),
		probeCache: make(map[string]probeCacheEntry),
	}

	for _, v := range viewers {
		v.token = sessionToken("c:"+shortSum(v.client), hash, v.fileID, 0)
		task := &Task{
			Token:           v.token,
			Hash:            hash,
			FileID:          v.fileID,
			Config:          conf,
			LastSentSegment: -1,
			lastActive:      time.Now().UTC(),
			Probe:           multiclientProbe(),
		}
		v.runner = &viewerRunner{token: v.token}
		task.runner = v.runner
		task.initMP4 = []byte("init:" + v.token)
		task.variant = &HLSVariantInfo{Codecs: "avc1.640028,mp4a.40.2", Width: 1920, Height: 1080}
		service.tasks[v.token] = task
	}

	router := gin.New()
	service.SetupRoute(router)
	return router, service
}

// Two viewers on one release, each on their own file, both pulling segments and both seeking.
// This is the scenario the whole session model exists for, and until now nothing exercised it:
// coverage was a single client through HTTP plus unit tests of the pieces.
func TestTwoViewersOfOneTorrentNeverCrossStreams(t *testing.T) {
	const hash = "hash"
	viewers := []*viewer{
		{client: "living-room", fileID: "1"},
		{client: "bedroom", fileID: "2"},
	}
	router, _ := newMulticlientService(t, hash, viewers)

	if viewers[0].token == viewers[1].token {
		t.Fatal("two viewers of one release collapsed into one session")
	}

	var wrongBytes, badStatus atomic.Int64
	var mu sync.Mutex
	seen := make(map[string]string)

	const rounds = 40
	startTogether(len(viewers)*2, func(worker int) {
		v := viewers[worker%len(viewers)]
		seeking := worker >= len(viewers)

		for round := range rounds {
			index := round
			if seeking {
				// A viewer scrubbing jumps around instead of walking forward.
				index = (rounds - round) % rounds
			}
			target := fmt.Sprintf("/gst/%s/c/%s/seg/%d.m4s?audio=0", hash, v.token, index)

			response := httptest.NewRecorder()
			router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, target, nil))
			if response.Code != http.StatusOK {
				badStatus.Add(1)
				continue
			}

			body := response.Body.String()
			// The payload names the session that produced it.
			if !strings.Contains(body, v.token) {
				wrongBytes.Add(1)
			}
			mu.Lock()
			seen[v.client] = body
			mu.Unlock()
		}
	})

	if got := badStatus.Load(); got != 0 {
		t.Fatalf("%d segment requests failed while two viewers shared the release", got)
	}
	if got := wrongBytes.Load(); got != 0 {
		t.Fatalf("%d segments carried another session's bytes", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if seen[viewers[0].client] == seen[viewers[1].client] {
		t.Fatal("both viewers ended up with identical bytes")
	}
}

// Each viewer's init segment and playlist belong to their own pipeline. A player that got the
// other viewer's init would decode garbage without a single error being logged anywhere.
func TestEachViewerGetsTheirOwnInitAndPlaylist(t *testing.T) {
	const hash = "hash"
	viewers := []*viewer{
		{client: "living-room", fileID: "1"},
		{client: "bedroom", fileID: "2"},
	}
	router, _ := newMulticlientService(t, hash, viewers)

	get := func(target string) *httptest.ResponseRecorder {
		t.Helper()
		response := httptest.NewRecorder()
		router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, target, nil))
		if response.Code != http.StatusOK {
			t.Fatalf("GET %s status=%d body=%q", target, response.Code, response.Body.String())
		}
		return response
	}

	for _, v := range viewers {
		master := get(fmt.Sprintf("/gst/%s/master.m3u8?index=%s&audio=0&client=%s", hash, v.fileID, v.client))
		variantURL := lastPlaylistURI(t, master.Body.String())
		if !strings.Contains(variantURL, "/c/"+v.token+"/") {
			t.Fatalf("%s was pointed at %q, which is not their session", v.client, variantURL)
		}

		base, err := url.Parse(variantURL)
		if err != nil {
			t.Fatal(err)
		}
		reference, err := url.Parse("init.mp4?audio=0")
		if err != nil {
			t.Fatal(err)
		}
		init := get(base.ResolveReference(reference).String())
		if !strings.Contains(init.Body.String(), v.token) {
			t.Fatalf("%s got an init segment belonging to another session", v.client)
		}
	}
}

// With more than one session on a release, the legacy URL that carries no token is ambiguous.
// Guessing would hand a viewer another viewer's stream, so it has to refuse.
func TestLegacyURLRefusesWhenTheReleaseHasSeveralViewers(t *testing.T) {
	const hash = "hash"
	viewers := []*viewer{{client: "living-room", fileID: "1"}}
	router, service := newMulticlientService(t, hash, viewers)

	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/gst/"+hash+"/video.m3u8?audio=0", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("a sole session should still answer the legacy URL, status=%d", response.Code)
	}

	second, _ := newTrackedTask("second", time.Now().UTC())
	second.Hash = hash
	service.tasks[second.Token] = second

	response = httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/gst/"+hash+"/video.m3u8?audio=0", nil))
	if response.Code != http.StatusNotFound {
		t.Fatalf("ambiguous legacy URL status=%d, want 404", response.Code)
	}

	// The token-carrying URL is unaffected by how many other viewers there are.
	response = httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet,
		fmt.Sprintf("/gst/%s/c/%s/seg/0.m4s?audio=0", hash, viewers[0].token), nil))
	if response.Code != http.StatusOK {
		t.Fatalf("session URL status=%d with a second viewer present, want 200", response.Code)
	}
}
