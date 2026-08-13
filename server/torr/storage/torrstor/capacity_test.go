package torrstor

import (
	"testing"

	"server/settings"
)

func cacheWithReaders(base int64, active int) *Cache {
	cache := &Cache{readers: make(map[*Reader]struct{})}
	cache.capacity.Store(base)
	for range active {
		cache.readers[&Reader{cache: cache, isUse: true}] = struct{}{}
	}
	return cache
}

// A reader's window is capacity/readers wide, so a fixed capacity means every new viewer
// shrinks everybody's buffer. Scaling keeps each viewer's window at the configured size.
func TestEffectiveCapacityScalesWithViewers(t *testing.T) {
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
	// The byte ceiling wins over the reader multiplier.
	const large = 200 << 20
	if got := cacheWithReaders(large, 4).effectiveCapacity(); got != maxCapacityBytes {
		t.Fatalf("effectiveCapacity=%d, want the ceiling %d", got, maxCapacityBytes)
	}

	// But it never cuts a cache somebody deliberately configured above it.
	const huge = maxCapacityBytes * 2
	if got := cacheWithReaders(huge, 4).effectiveCapacity(); got != huge {
		t.Fatalf("effectiveCapacity=%d, want the configured %d", got, huge)
	}

	// An unset capacity stays unset; Init decides what to do with it.
	if got := cacheWithReaders(0, 3).effectiveCapacity(); got != 0 {
		t.Fatalf("effectiveCapacity=%d, want 0 to be left alone", got)
	}
}

// Five viewers on a 25 connection budget get five blocks each, which is not enough to hold
// a stream. The floor is what keeps a viewer playing when another one appears.
func TestConnectionsPerReaderHasAFloor(t *testing.T) {
	previous := settings.BTsets
	settings.BTsets = &settings.BTSets{ConnectionsLimit: 25}
	t.Cleanup(func() { settings.BTsets = previous })

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
