package torrstor

import (
	"context"
	"io"
	"slices"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/anacrolix/torrent"

	"server/settings"
)

// fakeFile is a file extent with no torrent client behind it.
type fakeFile struct {
	offset int64
	length int64
}

func (f fakeFile) Offset() int64             { return f.offset }
func (f fakeFile) Length() int64             { return f.length }
func (f fakeFile) NewReader() torrent.Reader { return nil }
func (f fakeFile) HasInfo() bool             { return true }
func (f fakeFile) HasFiles() bool            { return true }

// fakeReaderBackend stands in for the anacrolix reader that readerOn/readerOff drive.
type fakeReaderBackend struct {
	mu       sync.Mutex
	position int64
}

func (b *fakeReaderBackend) Read([]byte) (int, error) { return 0, io.EOF }
func (b *fakeReaderBackend) Close() error             { return nil }
func (b *fakeReaderBackend) SetReadahead(int64)       {}
func (b *fakeReaderBackend) SetResponsive()           {}

func (b *fakeReaderBackend) ReadContext(context.Context, []byte) (int, error) {
	return 0, io.EOF
}

func (b *fakeReaderBackend) Seek(offset int64, whence int) (int64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch whence {
	case io.SeekStart:
		b.position = offset
	case io.SeekCurrent:
		b.position += offset
	case io.SeekEnd:
		b.position = offset
	}
	return b.position, nil
}

// fakePieceSource records what the sweep asked the torrent client to do.
type fakePieceSource struct {
	mu         sync.Mutex
	priorities map[int]PiecePriority
}

func newFakePieceSource() *fakePieceSource {
	return &fakePieceSource{priorities: make(map[int]PiecePriority)}
}

func (f *fakePieceSource) Name() string { return "fake" }

func (f *fakePieceSource) PriorityAt(index int) PiecePriority {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.priorities[index]
}

func (f *fakePieceSource) SetPriorityAt(index int, priority PiecePriority) {
	f.mu.Lock()
	f.priorities[index] = priority
	f.mu.Unlock()
}

func (f *fakePieceSource) UpdateCompletionAt(int) {}

type sweepOption func(*sweepFixture)

type sweepFixture struct {
	cache   *Cache
	source  *fakePieceSource
	readers []*Reader
	staleBy []int64
}

// withReader adds an active reader at a byte offset inside the file.
func withReader(offset int64, staleSeconds int64) sweepOption {
	return func(f *sweepFixture) {
		r := &Reader{
			cache: f.cache,
			file:  fakeFile{offset: 0, length: 1 << 30},
		}
		r.Reader = &fakeReaderBackend{}
		r.offset.Store(offset)
		r.readahead.Store(1 << 20)
		r.isUse.Store(true)
		r.lastAccess.Store(time.Now().Unix() - staleSeconds)
		f.cache.readers[r] = struct{}{}
		f.readers = append(f.readers, r)
		f.staleBy = append(f.staleBy, staleSeconds)
	}
}

func withPieces(count int, size int64) sweepOption {
	return func(f *sweepFixture) {
		for i := range count {
			piece := &Piece{Id: i, cache: f.cache}
			piece.Size.Store(size)
			piece.Accessed.Store(int64(i))
			f.cache.pieces[i] = piece
		}
	}
}

func newSweepFixture(t *testing.T, options ...sweepOption) *sweepFixture {
	t.Helper()

	previous := settings.BTsets
	settings.BTsets = &settings.BTSets{
		ConnectionsLimit: 60,
		ReaderReadAHead:  50,
	}
	t.Cleanup(func() { settings.BTsets = previous })

	fixture := &sweepFixture{
		cache:  &Cache{pieces: make(map[int]*Piece), readers: make(map[*Reader]struct{})},
		source: newFakePieceSource(),
	}
	fixture.cache.pieceLength = 1 << 20
	fixture.cache.pieceCount = 1024
	fixture.cache.capacity.Store(64 << 20)
	fixture.cache.setPieceSource(fixture.source)

	for _, option := range options {
		option(fixture)
	}
	return fixture
}

// restore puts the readers back where they started. lastAccess has to be re-stamped against
// the current time, not just restored to a fixed value: checkReader compares it to time.Now(),
// so a test that runs longer than the idle timeout silently turns every reader off underneath
// itself and starts measuring a different cache.
func (f *sweepFixture) restore() {
	now := time.Now().Unix()
	for i, r := range f.readers {
		r.isUse.Store(true)
		r.lastAccess.Store(now - f.staleBy[i])
	}
}

func (f *sweepFixture) removableIDs() []int {
	ids := make([]int, 0)
	for _, p := range f.cache.getRemPieces() {
		ids = append(ids, p.Id)
	}
	sort.Ints(ids)
	return ids
}

// The sweep used to decide and measure in one pass: checkReader flips isUse, and a reader's
// window is the capacity divided by the number of active readers. So each reader got a
// different divisor depending on where in the map iteration it was reached — and Go randomises
// that order deliberately. The same cache state has to produce the same answer every time.
func TestSweepResultDoesNotDependOnMapOrder(t *testing.T) {
	// Six readers, not four. Below maxCapacityReaders the capacity scales with the reader
	// count and the division in getOffsetRange cancels out, so the divisor cannot be observed
	// at all — a test with four readers passes on the single-pass code and proves nothing.
	// Past the ceiling the window really is capacity/readers, and the count is what the two
	// passes exist to settle first.
	fixture := newSweepFixture(t,
		withPieces(512, 1<<20),
		withReader(16<<20, 0),
		withReader(64<<20, 0),
		withReader(128<<20, 0),
		withReader(200<<20, 120),
		withReader(300<<20, 120),
		withReader(400<<20, 120),
	)

	var reference []int
	for run := range 200 {
		fixture.restore()
		got := fixture.removableIDs()
		if run == 0 {
			reference = got
			continue
		}
		if !slices.Equal(got, reference) {
			t.Fatalf("run %d evicted %v, the first run evicted %v — the answer depends on map order",
				run, got, reference)
		}
	}
}

// Reader.Close kicks off a sweep in its own goroutine, so a household switching episodes
// starts several at once. Two sweeps interleaving hand the torrent contradictory priorities
// and both write c.filled.
func TestOnlyOneSweepRunsAtATime(t *testing.T) {
	fixture := newSweepFixture(t, withPieces(64, 1<<20), withReader(8<<20, 0))

	const sweepers = 8
	var concurrent, peak int
	var mu sync.Mutex

	startTogether(sweepers, func(int) {
		fixture.cache.muSweep.Lock()
		mu.Lock()
		concurrent++
		if concurrent > peak {
			peak = concurrent
		}
		mu.Unlock()

		time.Sleep(time.Millisecond)

		mu.Lock()
		concurrent--
		mu.Unlock()
		fixture.cache.muSweep.Unlock()
	})

	mu.Lock()
	defer mu.Unlock()
	if peak != 1 {
		t.Fatalf("%d sweeps were inside the pass at once", peak)
	}
}

// The real thing: concurrent sweeps against readers being served. Nothing may tear, and the
// pass must still hand out priorities for the piece each reader is sitting on.
func TestSweepIsSafeAgainstReadersBeingServed(t *testing.T) {
	fixture := newSweepFixture(t,
		withPieces(128, 1<<20),
		withReader(4<<20, 0),
		withReader(48<<20, 0),
		withReader(96<<20, 0),
	)

	startTogether(9, func(worker int) {
		for range 100 {
			switch worker % 3 {
			case 0:
				fixture.cache.getRemPieces()
			case 1:
				// Every reader stays warm: an idle reader drops out of the sweep, and this
				// test is about what the sweep does while people are watching.
				fixture.restore()
				fixture.readers[worker%len(fixture.readers)].offset.Add(1 << 16)
			default:
				_ = fixture.cache.GetState()
			}
		}
	})

	// Settle first: asserting on priorities while other workers are still moving readers makes
	// the check race the code it is checking, and a test that fails one run in ten is worse
	// than no test. The concurrent phase above is guarded by -race; this is the behaviour part.
	fixture.restore()
	fixture.cache.getRemPieces()

	// The piece a viewer is sitting on must be the client's most urgent one: if the pass
	// silently stopped handing out priorities, playback would stall with no error anywhere.
	// Checked on a reader past the file head — the first 8 MB are protected from eviction on
	// their own (isIdInFileBE), so setLoadPriority deliberately skips a reader sitting there.
	watching := fixture.readers[1]
	if got := fixture.source.PriorityAt(watching.getReaderPiece()); got != PriorityNow {
		t.Fatalf("the piece a viewer is reading has priority %d, want %d", got, PriorityNow)
	}
}

// A piece inside any active reader's window is not evictable. That is what keeps one viewer's
// buffer from being thrown away by another viewer's sweep.
func TestSweepKeepsPiecesInsideAReadersWindow(t *testing.T) {
	fixture := newSweepFixture(t, withPieces(256, 1<<20), withReader(100<<20, 0))

	removable := fixture.removableIDs()
	readerPiece := fixture.readers[0].getReaderPiece()

	if slices.Contains(removable, readerPiece) {
		t.Fatalf("piece %d is where the reader is sitting and was offered for eviction", readerPiece)
	}
	if len(removable) == 0 {
		t.Fatal("a single reader protected the whole cache, leaving nothing to evict")
	}
}
