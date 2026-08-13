package torr

import (
	"testing"

	sets "server/settings"
)

// PreloadCache is a percentage of the cache, so raising the cache raises how long a viewer
// waits before playback may begin - and the bytes are fetched from the head of the file,
// which is not where somebody resuming mid-episode needs them. The cap is what keeps a
// cache size change from becoming a startup regression.
func TestPreloadSizeIsCappedInBytes(t *testing.T) {
	previous := sets.BTsets
	t.Cleanup(func() { sets.BTsets = previous })

	// The percentage is applied in float32 upstream, so the exact byte count drifts a
	// little; what matters is the magnitude.
	const tolerance = 1 << 10

	for _, test := range []struct {
		name      string
		cacheSize int64
		percent   int
		want      int64
	}{
		{"shipped defaults stay well under the cap", 256 << 20, 12, 256 << 20 / 100 * 12},
		{"a large cache is capped", 4 << 30, 50, preloadMaxBytes},
		{"upstream sizing is untouched", 64 << 20, 50, 32 << 20},
	} {
		sets.BTsets = &sets.BTSets{CacheSize: test.cacheSize, PreloadCache: test.percent}
		got := preloadSize()
		if got < test.want-tolerance || got > test.want+tolerance {
			t.Fatalf("%s: preloadSize()=%d, want %d", test.name, got, test.want)
		}
	}

	sets.BTsets = &sets.BTSets{CacheSize: 256 << 20, PreloadCache: 0}
	if got := preloadSize(); got != 0 {
		t.Fatalf("preloadSize()=%d, want 0 to mean no preload", got)
	}
}
