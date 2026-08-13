package settings

import (
	"path/filepath"
	"testing"
)

func withDefaults(t *testing.T, path string) *BTSets {
	t.Helper()

	previousReadOnly, previousPath, previousSets := ReadOnly, Path, BTsets
	// ReadOnly keeps SetDefaultConfig from reaching the database, which a unit test has
	// not opened.
	ReadOnly, Path = true, path
	t.Cleanup(func() {
		ReadOnly, Path, BTsets = previousReadOnly, previousPath, previousSets
	})

	SetDefaultConfig()
	return BTsets
}

func TestDefaultConfigSizesForAHousehold(t *testing.T) {
	sets := withDefaults(t, t.TempDir())

	if sets.CacheSize != defaultCacheSize {
		t.Fatalf("CacheSize=%d, want %d", sets.CacheSize, defaultCacheSize)
	}
	if sets.ConnectionsLimit != defaultConnectionsLimit {
		t.Fatalf("ConnectionsLimit=%d, want %d", sets.ConnectionsLimit, defaultConnectionsLimit)
	}

	// The preload is a percentage of the cache and the viewer waits through all of it, so
	// the two defaults are one decision: a bigger cache must not mean a longer black
	// screen. Roughly 32 MB is what starts in time.
	preload := sets.CacheSize / 100 * int64(sets.PreloadCache)
	if preload < 24<<20 || preload > 40<<20 {
		t.Fatalf("preload works out to %d MB, want it near 32", preload>>20)
	}
}

// UseDisk is silently forced off without a path, so shipping one without the other would
// look like the setting had been ignored.
func TestDefaultConfigPairsDiskCacheWithAPath(t *testing.T) {
	root := t.TempDir()
	sets := withDefaults(t, root)

	if !sets.UseDisk {
		t.Fatal("UseDisk should be on by default")
	}
	if want := filepath.Join(root, defaultCacheDirName); sets.TorrentsSavePath != want {
		t.Fatalf("TorrentsSavePath=%q, want %q", sets.TorrentsSavePath, want)
	}
	// Cache files outlive the torrent that created them, so without this the disk fills.
	if !sets.RemoveCacheOnDrop {
		t.Fatal("a disk cache that is never cleaned fills the disk")
	}
}

func TestDefaultConfigLeavesDiskCacheOffWithoutAPath(t *testing.T) {
	sets := withDefaults(t, "")

	if sets.UseDisk || sets.TorrentsSavePath != "" {
		t.Fatalf("UseDisk=%v path=%q, want the disk cache left alone", sets.UseDisk, sets.TorrentsSavePath)
	}
}

// A stored config that predates a setting arrives here with a zero in it, and must land on
// the same value a fresh install gets rather than the old upstream one.
func TestFailsafeFillsZeroesWithTheCurrentDefaults(t *testing.T) {
	sets := &BTSets{}
	sets.applyFailsafeDefaults()

	if sets.CacheSize != defaultCacheSize || sets.ConnectionsLimit != defaultConnectionsLimit {
		t.Fatalf("failsafe left CacheSize=%d ConnectionsLimit=%d", sets.CacheSize, sets.ConnectionsLimit)
	}

	// A deliberate choice is not a zero, and must survive untouched.
	chosen := &BTSets{CacheSize: 32 << 20, ConnectionsLimit: 10, ReaderReadAHead: 50}
	chosen.applyFailsafeDefaults()
	if chosen.CacheSize != 32<<20 || chosen.ConnectionsLimit != 10 || chosen.ReaderReadAHead != 50 {
		t.Fatalf("failsafe overwrote a configured value: %+v", chosen)
	}
}
