package torrstor

import (
	"sync/atomic"
	"time"

	"github.com/anacrolix/torrent/storage"
	"server/settings"
)

// Size, Complete and Accessed are written by the torrent download path and read by the
// eviction sweep and by GetState, which every client's heartbeat calls. Those run on
// different goroutines with no lock between them - the per-piece mutex covers the buffer,
// not this bookkeeping - so the fields carry their own synchronisation.
//
// They are not serialised anywhere; GetState copies them into state.ItemState by hand.
type Piece struct {
	storage.PieceImpl `json:"-"`

	Id       int `json:"-"`
	Size     atomic.Int64
	Complete atomic.Bool
	Accessed atomic.Int64

	mPiece *MemPiece  `json:"-"`
	dPiece *DiskPiece `json:"-"`

	cache *Cache `json:"-"`
}

func NewPiece(id int, cache *Cache) *Piece {
	p := &Piece{
		Id:    id,
		cache: cache,
	}

	if !settings.BTsets.UseDisk {
		p.mPiece = NewMemPiece(p)
	} else {
		p.dPiece = NewDiskPiece(p)
	}
	return p
}

func (p *Piece) WriteAt(b []byte, off int64) (n int, err error) {
	if !settings.BTsets.UseDisk {
		return p.mPiece.WriteAt(b, off)
	} else {
		return p.dPiece.WriteAt(b, off)
	}
}

func (p *Piece) ReadAt(b []byte, off int64) (n int, err error) {
	if !settings.BTsets.UseDisk {
		return p.mPiece.ReadAt(b, off)
	} else {
		return p.dPiece.ReadAt(b, off)
	}
}

func (p *Piece) MarkComplete() error {
	p.Complete.Store(true)
	return nil
}

func (p *Piece) MarkNotComplete() error {
	p.Complete.Store(false)
	return nil
}

func (p *Piece) Completion() storage.Completion {
	return storage.Completion{
		Complete: p.Complete.Load(),
		Ok:       true,
	}
}

// noteWrite records n freshly written bytes, clamped to one piece.
func (p *Piece) noteWrite(n int) {
	if size := p.Size.Add(int64(n)); size > p.cache.pieceLength {
		p.Size.Store(p.cache.pieceLength)
	}
	p.Accessed.Store(time.Now().Unix())
}

func (p *Piece) noteRelease() {
	p.Size.Store(0)
	p.Complete.Store(false)
}

func (p *Piece) Release() {
	if !settings.BTsets.UseDisk {
		p.mPiece.Release()
	} else {
		p.dPiece.Release()
	}
	// if !p.cache.isClosed {
	if source := p.cache.pieceSource(); source != nil {
		source.SetPriorityAt(p.Id, PriorityNone)
		source.UpdateCompletionAt(p.Id)
	}
	//}
}
