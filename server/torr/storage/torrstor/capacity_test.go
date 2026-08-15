package torrstor

import (
	"testing"

	"server/settings"
)

func withBTSets(t *testing.T, sets *settings.BTSets) {
	t.Helper()
	previous := settings.BTsets
	settings.BTsets = sets
	t.Cleanup(func() { settings.BTsets = previous })
}

func cacheWithReaders(base int64, active int) *Cache {
	cache := &Cache{readers: make(map[*Reader]struct{})}
	cache.capacity.Store(base)
	for range active {
		r := &Reader{cache: cache}
		r.isUse.Store(true)
		cache.readers[r] = struct{}{}
	}
	return cache
}

// A reader's window is capacity/readers wide, so a fixed capacity means every new viewer
// shrinks everybody's buffer. Scaling keeps each viewer's window at the configured size.
func TestEffectiveCapacityScalesWithViewers(t *testing.T) {
	withBTSets(t, &settings.BTSets{})
	const base = 64 << 20

	for _, test := range []struct {
		active int
		want   int64
	}{
		{0, base},
		{1, base},
		{2, 2 * base},
		{maxCapacityReaders, maxCapacityReaders * base},
		{maxCapacityReaders + 3, maxCapacityReaders * base},
	} {
		got := cacheWithReaders(base, test.active).effectiveCapacity()
		if got != test.want {
			t.Fatalf("effectiveCapacity(%d active)=%d, want %d", test.active, got, test.want)
		}
	}
}

func TestEffectiveCapacityRespectsCeilings(t *testing.T) {
	withBTSets(t, &settings.BTSets{})

	// The byte ceiling wins over the reader multiplier.
	const large = 200 << 20
	if got := cacheWithReaders(large, 4).effectiveCapacity(); got != maxCapacityBytesInRAM {
		t.Fatalf("effectiveCapacity=%d, want the ceiling %d", got, maxCapacityBytesInRAM)
	}

	// But it never cuts a cache somebody deliberately configured above it.
	const huge = maxCapacityBytesInRAM * 2
	if got := cacheWithReaders(huge, 4).effectiveCapacity(); got != huge {
		t.Fatalf("effectiveCapacity=%d, want the configured %d", got, huge)
	}

	// An unset capacity stays unset; Init decides what to do with it.
	if got := cacheWithReaders(0, 3).effectiveCapacity(); got != 0 {
		t.Fatalf("effectiveCapacity=%d, want 0 to be left alone", got)
	}
}

// The ceiling is about what is being spent, and a disk cache spends something there is a
// lot more of. Without this the shipped 256 MB default would sit against the memory
// ceiling and scale barely twice.
func TestEffectiveCapacityCeilingFollowsTheCacheLocation(t *testing.T) {
	const base = 256 << 20

	withBTSets(t, &settings.BTSets{})
	inRAM := cacheWithReaders(base, maxCapacityReaders).effectiveCapacity()
	if inRAM != maxCapacityBytesInRAM {
		t.Fatalf("in memory effectiveCapacity=%d, want %d", inRAM, maxCapacityBytesInRAM)
	}

	withBTSets(t, &settings.BTSets{UseDisk: true})
	onDisk := cacheWithReaders(base, maxCapacityReaders).effectiveCapacity()
	if onDisk != base*maxCapacityReaders {
		t.Fatalf("on disk effectiveCapacity=%d, want the full %d", onDisk, base*maxCapacityReaders)
	}
	if onDisk <= inRAM {
		t.Fatal("a disk cache should be allowed to grow past the memory ceiling")
	}
}

// Five viewers on a 25 connection budget get five blocks each, which is not enough to hold
// a stream. The floor is what keeps a viewer playing when another one appears.
func TestConnectionsPerReaderHasAFloor(t *testing.T) {
	withBTSets(t, &settings.BTSets{ConnectionsLimit: 25})

	if got := connectionsPerReader(1); got != 25 {
		t.Fatalf("connectionsPerReader(1)=%d, want the whole budget", got)
	}
	if got := connectionsPerReader(2); got != 12 {
		t.Fatalf("connectionsPerReader(2)=%d, want an even split while it is above the floor", got)
	}
	for _, active := range []int{0, 5, 12} {
		if got := connectionsPerReader(active); got < minConnectionsPerReader {
			t.Fatalf("connectionsPerReader(%d)=%d, want at least the floor %d", active, got, minConnectionsPerReader)
		}
	}
}
