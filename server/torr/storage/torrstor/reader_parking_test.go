package torrstor

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"
)

// blockingBackend holds a read open until the test lets it finish, and records every seek
// that arrived while it was held. That is the whole oracle: the bytes a reader returns are
// only correct if nothing moved it between the caller's request and the read.
type blockingBackend struct {
	entered chan struct{}
	release chan struct{}

	mu              sync.Mutex
	position        int64
	seeksDuringRead []int64
	reading         bool
}

func newBlockingBackend(position int64) *blockingBackend {
	return &blockingBackend{
		entered:  make(chan struct{}, 1),
		release:  make(chan struct{}),
		position: position,
	}
}

func (b *blockingBackend) Close() error       { return nil }
func (b *blockingBackend) SetReadahead(int64) {}
func (b *blockingBackend) SetResponsive()     {}

func (b *blockingBackend) Read(p []byte) (int, error) {
	b.mu.Lock()
	b.reading = true
	b.mu.Unlock()

	b.entered <- struct{}{}
	<-b.release

	b.mu.Lock()
	defer b.mu.Unlock()
	b.reading = false
	// The bytes name the position they came from, which is how a caller that was handed the
	// head of the file instead of its own position is caught.
	return copy(p, []byte(formatPosition(b.position))), nil
}

func (b *blockingBackend) ReadContext(_ context.Context, p []byte) (int, error) {
	return b.Read(p)
}

func (b *blockingBackend) Seek(offset int64, whence int) (int64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.reading {
		b.seeksDuringRead = append(b.seeksDuringRead, offset)
	}
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

func (b *blockingBackend) seeksWhileReading() []int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]int64(nil), b.seeksDuringRead...)
}

func formatPosition(offset int64) string {
	digits := []byte{}
	if offset == 0 {
		return "0"
	}
	for offset > 0 {
		digits = append([]byte{byte('0' + offset%10)}, digits...)
		offset /= 10
	}
	return string(digits)
}

// The sweep parks a reader it considers idle at byte 0 so the cache can evict around it.
// "Idle" is decided from a stamp written only after a read returns, so a reader waiting on
// the swarm — the one case where being moved does real damage — is exactly what it picks.
// The read then resumes and hands the head of the file to a caller that asked for the middle
// of it. Downstream that surfaces as a demuxer reporting the file as corrupt.
func TestSweepDoesNotParkAReaderWithAReadInFlight(t *testing.T) {
	const position = 900 << 20

	cache := &Cache{pieceLength: 1 << 20, readers: make(map[*Reader]struct{})}
	cache.capacity.Store(256 << 20)

	busy := &Reader{cache: cache, file: fakeFile{offset: 0, length: 1 << 30}}
	backend := newBlockingBackend(position)
	busy.Reader = backend
	busy.offset.Store(position)
	busy.readahead.Store(1 << 20)
	busy.isUse.Store(true)
	// Stale enough that checkReader wants it gone, which is the whole point: a read that
	// blocks longer than the idle threshold is a slow swarm, not an abandoned viewer.
	busy.lastAccess.Store(time.Now().Unix() - 120)
	cache.readers[busy] = struct{}{}

	// checkReader only parks anything when the torrent has more than one reader — which is
	// why this needs a second viewer present to reproduce at all.
	other := &Reader{cache: cache, file: fakeFile{offset: 0, length: 1 << 30}}
	other.Reader = &fakeReaderBackend{}
	other.isUse.Store(true)
	other.lastAccess.Store(time.Now().Unix())
	cache.readers[other] = struct{}{}

	got := make(chan string, 1)
	go func() {
		buffer := make([]byte, 32)
		n, _ := busy.Read(buffer)
		got <- string(buffer[:n])
	}()

	<-backend.entered
	// The sweep runs while the read sits on the swarm.
	busy.checkReader()
	close(backend.release)

	select {
	case bytes := <-got:
		if want := formatPosition(position); bytes != want {
			t.Fatalf("the read returned bytes from position %s, want %s", bytes, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the read never finished")
	}

	if seeks := backend.seeksWhileReading(); len(seeks) != 0 {
		t.Fatalf("the reader was moved to %v while it was serving a read", seeks)
	}
	if !busy.isUse.Load() {
		t.Fatal("a reader with a read in flight was marked unused")
	}
}

// The guard must not make a genuinely idle reader un-parkable, or the cache loses the only
// thing that lets it evict around a viewer who walked away.
func TestSweepStillParksAnIdleReader(t *testing.T) {
	cache := &Cache{pieceLength: 1 << 20, readers: make(map[*Reader]struct{})}
	cache.capacity.Store(256 << 20)

	idle := &Reader{cache: cache, file: fakeFile{offset: 0, length: 1 << 30}}
	backend := &fakeReaderBackend{position: 900 << 20}
	idle.Reader = backend
	idle.offset.Store(900 << 20)
	idle.isUse.Store(true)
	idle.lastAccess.Store(time.Now().Unix() - 120)
	cache.readers[idle] = struct{}{}

	active := &Reader{cache: cache, file: fakeFile{offset: 0, length: 1 << 30}}
	active.Reader = &fakeReaderBackend{}
	active.isUse.Store(true)
	active.lastAccess.Store(time.Now().Unix())
	cache.readers[active] = struct{}{}

	idle.checkReader()

	if idle.isUse.Load() {
		t.Fatal("an idle reader was left holding its piece claims")
	}
	if got, _ := backend.Seek(0, io.SeekCurrent); got != 0 {
		t.Fatalf("idle reader parked at %d, want 0", got)
	}

	// And it comes back where it left off the moment it is used again.
	idle.beginIO()
	defer idle.endIO()
	if got, _ := backend.Seek(0, io.SeekCurrent); got != 900<<20 {
		t.Fatalf("resumed reader is at %d, want its own position", got)
	}
}
