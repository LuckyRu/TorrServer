package torrstor

import "github.com/anacrolix/torrent"

// The cache talks to the torrent client through these interfaces rather than through
// *torrent.Torrent and *torrent.File directly.
//
// This is not abstraction for its own sake. The eviction pass — getRemPieces, setLoadPriority,
// clearPriority — is where three separate concurrency defects have already happened, and until
// now no test could reach it at all: it dereferences a concrete torrent and a concrete file,
// neither of which can be built in a unit test. The pass that decides what a household of
// viewers keeps in cache was the one part of this package with no coverage of its own.

// PiecePriority mirrors the client's piece priorities, whose own type is unexported and so
// cannot appear in an interface. The values match one for one, because GetState reports them.
type PiecePriority int

const (
	PriorityNone PiecePriority = iota
	PriorityNormal
	PriorityHigh
	PriorityReadahead
	PriorityNext
	PriorityNow
)

// torrentPieces is the slice of the torrent client that the cache needs.
type torrentPieces interface {
	Name() string
	PriorityAt(index int) PiecePriority
	SetPriorityAt(index int, priority PiecePriority)
	UpdateCompletionAt(index int)
}

// readerFile is the slice of a torrent file that a Reader needs. Offset is measured from the
// start of the torrent, which is the only frame in which positions in different files compare.
type readerFile interface {
	Offset() int64
	Length() int64
	NewReader() torrent.Reader
	// HasInfo reports whether the torrent behind the file is still usable for reading.
	HasInfo() bool
	// HasFiles reports whether the torrent has a file list, which decides whether the
	// underlying reader owns anything worth closing.
	HasFiles() bool
}

type anacrolixTorrent struct{ inner *torrent.Torrent }

func (a anacrolixTorrent) Name() string { return a.inner.Name() }

func (a anacrolixTorrent) PriorityAt(index int) PiecePriority {
	return PiecePriority(a.inner.PieceState(index).Priority)
}

func (a anacrolixTorrent) SetPriorityAt(index int, priority PiecePriority) {
	switch priority {
	case PriorityNow:
		a.inner.Piece(index).SetPriority(torrent.PiecePriorityNow)
	case PriorityNext:
		a.inner.Piece(index).SetPriority(torrent.PiecePriorityNext)
	case PriorityReadahead:
		a.inner.Piece(index).SetPriority(torrent.PiecePriorityReadahead)
	case PriorityHigh:
		a.inner.Piece(index).SetPriority(torrent.PiecePriorityHigh)
	case PriorityNormal:
		a.inner.Piece(index).SetPriority(torrent.PiecePriorityNormal)
	default:
		a.inner.Piece(index).SetPriority(torrent.PiecePriorityNone)
	}
}

func (a anacrolixTorrent) UpdateCompletionAt(index int) {
	a.inner.Piece(index).UpdateCompletion()
}

type anacrolixFile struct{ inner *torrent.File }

func (a anacrolixFile) Offset() int64             { return a.inner.Offset() }
func (a anacrolixFile) Length() int64             { return a.inner.Length() }
func (a anacrolixFile) NewReader() torrent.Reader { return a.inner.NewReader() }

func (a anacrolixFile) HasInfo() bool {
	return a.inner.Torrent() != nil && a.inner.Torrent().Info() != nil
}

func (a anacrolixFile) HasFiles() bool {
	return a.inner.Torrent() != nil && len(a.inner.Torrent().Files()) > 0
}
