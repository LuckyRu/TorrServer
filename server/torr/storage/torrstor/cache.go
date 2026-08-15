package torrstor

import (
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anacrolix/torrent"

	"server/log"
	"server/settings"
	"server/torr/storage/state"
	"server/torr/utils"

	"github.com/anacrolix/torrent/metainfo"
	"github.com/anacrolix/torrent/storage"
)

type Cache struct {
	storage.TorrentImpl
	storage *Storage

	// capacity is the configured size for a single viewer; effectiveCapacity scales it.
	// Atomic because Init and AdjustRA write it while the eviction sweep reads it.
	capacity atomic.Int64
	// filled is recomputed by every heartbeat through GetState and by the eviction sweep,
	// and those run concurrently once a torrent feeds more than one stream.
	filled atomic.Int64
	hash   metainfo.Hash

	pieceLength int64
	pieceCount  int

	// pieces content is immutable after Init; muPieces guards the map
	// reference itself (nilled in Close), so holders of a snapshot may
	// safely iterate it without the lock
	pieces   map[int]*Piece
	muPieces sync.RWMutex

	readers   map[*Reader]struct{}
	muReaders sync.RWMutex

	isRemove atomic.Bool
	isClosed atomic.Bool
	muRemove sync.Mutex
	muSweep  sync.Mutex
	torrent  *torrent.Torrent
}

func NewCache(capacity int64, storage *Storage) *Cache {
	ret := &Cache{

		pieces:  make(map[int]*Piece),
		storage: storage,
		readers: make(map[*Reader]struct{}),
	}
	ret.capacity.Store(capacity)

	return ret
}

// maxCapacityReaders and maxCapacityBytes cap how far the cache grows with viewers.
//
// A reader's window is capacity/readers wide (see Reader.getOffsetRange), so without
// scaling every new viewer shrinks everybody's buffer: at a 4K bitrate a 64 MB cache split
// five ways is a couple of seconds of video each. Scaling with the reader count gives each
// viewer back the window a single viewer would have had.
//
// The ceilings are what keeps that bounded, and they matter because the cost is paid per
// torrent: several torrents playing at once multiply it.
//
// The byte ceiling depends on where the cache lives, because that decides what is being
// spent. In memory it is the process footprint and has to stay modest; on disk it is space
// that is plentiful and reclaimed when the torrent is dropped.
const (
	maxCapacityReaders    = 4
	maxCapacityBytesInRAM = 512 << 20
	maxCapacityOnDisk     = 4 << 30
)

func maxCapacityBytes() int64 {
	if settings.BTsets != nil && settings.BTsets.UseDisk {
		return maxCapacityOnDisk
	}
	return maxCapacityBytesInRAM
}

// minConnectionsPerReader is the floor under a viewer's share of the torrent's connection
// budget.
//
// An even split starves everybody once there are a few viewers: at the default limit of 25
// a fifth viewer leaves each of them five concurrent blocks, which is not enough to hold a
// stream. The floor means the total may exceed ConnectionsLimit — that is deliberate, and
// EstablishedConnsPerTorrent should be raised to match in a house with several viewers.
const minConnectionsPerReader = 8

func connectionsPerReader(activeReaders int) int {
	if activeReaders < 1 {
		activeReaders = 1
	}
	return max(minConnectionsPerReader, settings.BTsets.ConnectionsLimit/activeReaders)
}

// effectiveCapacity is the cache size for the number of viewers there are right now.
func (c *Cache) effectiveCapacity() int64 {
	base := c.capacity.Load()
	if base <= 0 {
		return base
	}

	readers := int64(c.GetUseReaders())
	if readers < 1 {
		readers = 1
	}
	if readers > maxCapacityReaders {
		readers = maxCapacityReaders
	}

	// Never below the configured size: someone who asked for a cache larger than the
	// ceiling meant it.
	return max(base, min(base*readers, maxCapacityBytes()))
}

func (c *Cache) Init(info *metainfo.Info, hash metainfo.Hash) {
	log.TLogln("Create cache for:", info.Name, hash.HexString())
	if c.capacity.Load() == 0 {
		c.capacity.Store(info.PieceLength * 4)
	}

	c.pieceLength = info.PieceLength
	c.pieceCount = info.NumPieces()
	c.hash = hash

	if settings.BTsets.UseDisk {
		name := filepath.Join(settings.BTsets.TorrentsSavePath, hash.HexString())
		err := os.MkdirAll(name, 0o777)
		if err != nil {
			log.TLogln("Error create dir:", err)
		}
	}

	for i := 0; i < c.pieceCount; i++ {
		c.pieces[i] = NewPiece(i, c)
	}
}

func (c *Cache) SetTorrent(torr *torrent.Torrent) {
	c.torrent = torr
}

func (c *Cache) getPieces() map[int]*Piece {
	c.muPieces.RLock()
	defer c.muPieces.RUnlock()
	return c.pieces
}

func (c *Cache) readersSnapshot() []*Reader {
	c.muReaders.RLock()
	defer c.muReaders.RUnlock()
	list := make([]*Reader, 0, len(c.readers))
	for r := range c.readers {
		list = append(list, r)
	}
	return list
}

func (c *Cache) Piece(m metainfo.Piece) storage.PieceImpl {
	if val, ok := c.getPieces()[m.Index()]; ok {
		return val
	}
	return &PieceFake{}
}

func (c *Cache) Close() error {
	if c.torrent != nil {
		log.TLogln("Close cache for:", c.torrent.Name(), c.hash)
	} else {
		log.TLogln("Close cache for:", c.hash)
	}
	c.isClosed.Store(true)

	c.storage.removeCache(c.hash)

	if settings.BTsets.RemoveCacheOnDrop {
		name := filepath.Join(settings.BTsets.TorrentsSavePath, c.hash.HexString())
		if name != "" && name != "/" {
			for _, v := range c.getPieces() {
				if v.dPiece != nil {
					os.Remove(v.dPiece.name)
				}
			}
			os.Remove(name)
		}
	}

	c.muReaders.Lock()
	c.readers = nil
	c.muReaders.Unlock()

	c.muPieces.Lock()
	c.pieces = nil
	c.muPieces.Unlock()

	utils.FreeOSMemGC()
	return nil
}

func (c *Cache) removePiece(piece *Piece) {
	if !c.isClosed.Load() {
		piece.Release()
	}
}

func (c *Cache) AdjustRA(readahead int64) {
	if c == nil {
		return
	}
	if settings.BTsets.CacheSize == 0 {
		c.capacity.Store(readahead * 3)
	}
	for _, r := range c.readersSnapshot() {
		r.SetReadahead(readahead)
	}
}

func (c *Cache) GetState() *state.CacheState {
	cState := new(state.CacheState)

	piecesState := make(map[int]state.ItemState, 0)
	var fill int64 = 0

	for _, p := range c.getPieces() {
		if size := p.Size.Load(); size > 0 {
			fill += size
			piecesState[p.Id] = state.ItemState{
				Id:        p.Id,
				Size:      size,
				Length:    c.pieceLength,
				Completed: p.Complete.Load(),
				Priority:  int(c.torrent.PieceState(p.Id).Priority),
			}
		}
	}

	readersState := make([]*state.ReaderState, 0)

	for _, r := range c.readersSnapshot() {
		rng := r.getPiecesRange()
		pc := r.getReaderPiece()
		readersState = append(readersState, &state.ReaderState{
			Start:  rng.Start,
			End:    rng.End,
			Reader: pc,
		})
	}

	c.filled.Store(fill)
	cState.Capacity = c.effectiveCapacity()
	cState.PiecesLength = c.pieceLength
	cState.PiecesCount = c.pieceCount
	cState.Hash = c.hash.HexString()
	cState.Filled = fill
	cState.Pieces = piecesState
	cState.Readers = readersState
	return cState
}

func (c *Cache) cleanPieces() {
	if c.isRemove.Load() || c.isClosed.Load() {
		return
	}

	// Protection against concurrent deletion
	if !c.muRemove.TryLock() {
		return // Cleanup is already in progress in another goroutine
	}
	defer c.muRemove.Unlock()

	c.isRemove.Store(true)
	defer func() { c.isRemove.Store(false) }()

	remPieces := c.getRemPieces()
	capacity := c.effectiveCapacity()
	if filled := c.filled.Load(); filled > capacity {
		rems := (filled-capacity)/c.pieceLength + 1
		for _, p := range remPieces {
			c.removePiece(p)
			rems--
			if rems <= 0 {
				utils.FreeOSMemGC()
				return
			}
		}
	}
}

// getRemPieces recomputes what may be evicted and refreshes piece priorities.
//
// One sweep at a time: every reader that closes kicks one off, so a household switching
// episodes starts several at once, and two sweeps interleaving hand the torrent
// contradictory piece priorities.
func (c *Cache) getRemPieces() []*Piece {
	c.muSweep.Lock()
	defer c.muSweep.Unlock()

	readers := c.readersSnapshot()

	// Settle which readers count as active before measuring anything. checkReader flips
	// isUse, and a reader's window is the capacity divided by the number of active ones -
	// so deciding and measuring in one pass gave each reader a different divisor, in map
	// order, which Go randomises deliberately.
	for _, r := range readers {
		r.checkReader()
	}

	// Collect read ranges from active readers
	ranges := make([]Range, 0, len(readers))
	for _, r := range readers {
		if r.isUse.Load() {
			ranges = append(ranges, r.getPiecesRange())
		}
	}
	ranges = mergeRange(ranges)

	piecesRemove := make([]*Piece, 0)
	fill := int64(0)

	// Determine which chunks can be deleted
	for id, p := range c.getPieces() {
		size := p.Size.Load()
		if size > 0 {
			fill += size
		}
		if len(ranges) > 0 {
			if !inRanges(ranges, id) {
				if size > 0 && !c.isIdInFileBE(ranges, id) {
					piecesRemove = append(piecesRemove, p)
				}
			}
		} else {
			// When preloading, clear everything except the beginning and end of the file
			if size > 0 && !c.isIdInFileBE(ranges, id) {
				piecesRemove = append(piecesRemove, p)
			}
		}
	}

	c.clearPriority()
	c.setLoadPriority(ranges)

	// Sort by last access time (oldest first)
	sort.Slice(piecesRemove, func(i, j int) bool {
		return piecesRemove[i].Accessed.Load() < piecesRemove[j].Accessed.Load()
	})

	c.filled.Store(fill)
	return piecesRemove
}

func (c *Cache) setLoadPriority(ranges []Range) {
	readers := c.readersSnapshot()
	pieces := c.getPieces()
	if len(readers) == 0 || pieces == nil {
		return
	}
	// Split the budget between viewers, not between readers: a reader that has gone idle
	// still counted in the divisor and quietly took bandwidth away from the one somebody
	// is actually watching.
	active := 0
	for _, r := range readers {
		if r.isUse.Load() {
			active++
		}
	}
	count := connectionsPerReader(active) // max concurrent loading blocks

	for _, r := range readers {
		if !r.isUse.Load() {
			continue
		}
		if c.isIdInFileBE(ranges, r.getReaderPiece()) {
			continue
		}
		readerPos := r.getReaderPiece()
		readerRAHPos := r.getReaderRAHPiece()
		end := r.getPiecesRange().End
		limit := 0
		for i := readerPos; i < end && limit < count; i++ {
			if !pieces[i].Complete.Load() {
				if i == readerPos {
					c.torrent.Piece(i).SetPriority(torrent.PiecePriorityNow)
				} else if i == readerPos+1 {
					c.torrent.Piece(i).SetPriority(torrent.PiecePriorityNext)
				} else if i > readerPos && i <= readerRAHPos {
					c.torrent.Piece(i).SetPriority(torrent.PiecePriorityReadahead)
				} else if i > readerRAHPos && i <= readerRAHPos+5 && c.torrent.PieceState(i).Priority != torrent.PiecePriorityHigh {
					c.torrent.Piece(i).SetPriority(torrent.PiecePriorityHigh)
				} else if i > readerRAHPos+5 && c.torrent.PieceState(i).Priority != torrent.PiecePriorityNormal {
					c.torrent.Piece(i).SetPriority(torrent.PiecePriorityNormal)
				}
				limit++
			}
		}
	}
}

func (c *Cache) isIdInFileBE(ranges []Range, id int) bool {
	// keep 8/16 MB
	FileRangeNotDelete := int64(c.pieceLength)
	if FileRangeNotDelete < 8<<20 {
		FileRangeNotDelete = 8 << 20
	}

	for _, rng := range ranges {
		ss := int(rng.File.Offset() / c.pieceLength)
		se := int((rng.File.Offset() + FileRangeNotDelete) / c.pieceLength)

		es := int((rng.File.Offset() + rng.File.Length() - FileRangeNotDelete) / c.pieceLength)
		ee := int((rng.File.Offset() + rng.File.Length()) / c.pieceLength)

		if id >= ss && id < se || id > es && id <= ee {
			return true
		}
	}
	return false
}

//////////////////
// Reader section
////////

// maxReadersPerTorrent bounds how many readers one torrent may have open at once.
//
// Both of a torrent's shared resources are divided by reader count. Piece priority is
// literally ConnectionsLimit/readers in setLoadPriority, and getRemPieces refuses to evict
// any piece that falls inside any reader's range. Enough readers at scattered offsets
// therefore starve each other for connections and, at the same time, leave the cache with
// nothing it is allowed to free.
//
// The limit sits above legitimate use — several viewers plus the short-lived readers that
// probing, cue reading and preload open — and below the runaway case of a client opening a
// range request per segment. It is a backstop, not a scheduling policy.
const maxReadersPerTorrent = 12

func (c *Cache) NewReader(file *torrent.File) (*Reader, error) {
	return newReader(file, c)
}

// admitReader publishes a reader if the torrent can still serve one.
//
// The count is checked while holding the same lock that publishes the reader, so
// simultaneous callers cannot all pass the check and overshoot the limit together.
func (c *Cache) admitReader(r *Reader) error {
	c.muReaders.Lock()
	defer c.muReaders.Unlock()

	if len(c.readers) >= maxReadersPerTorrent {
		return ErrTooManyReaders
	}
	c.readers[r] = struct{}{}
	return nil
}

func (c *Cache) GetUseReaders() int {
	if c == nil {
		return 0
	}
	c.muReaders.RLock()
	defer c.muReaders.RUnlock()
	readers := 0
	for reader := range c.readers {
		if reader.isUse.Load() {
			readers++
		}
	}
	return readers
}

func (c *Cache) Readers() int {
	if c == nil {
		return 0
	}
	c.muReaders.RLock()
	defer c.muReaders.RUnlock()
	return len(c.readers)
}

func (c *Cache) CloseReader(r *Reader) {
	r.cache.muReaders.Lock()
	delete(r.cache.readers, r)
	r.cache.muReaders.Unlock()
	// Reader.Close touches anacrolix internals, keep it outside muReaders
	r.Close()
	go c.clearPriority()
}

func (c *Cache) clearPriority() {
	if c.torrent == nil {
		return
	}
	time.Sleep(time.Second)
	ranges := make([]Range, 0)
	for _, r := range c.readersSnapshot() {
		r.checkReader()
		if r.isUse.Load() {
			ranges = append(ranges, r.getPiecesRange())
		}
	}
	ranges = mergeRange(ranges)

	for id := range c.getPieces() {
		if len(ranges) > 0 {
			if !inRanges(ranges, id) {
				if c.torrent.PieceState(id).Priority != torrent.PiecePriorityNone {
					c.torrent.Piece(id).SetPriority(torrent.PiecePriorityNone)
				}
			}
		} else {
			if c.torrent.PieceState(id).Priority != torrent.PiecePriorityNone {
				c.torrent.Piece(id).SetPriority(torrent.PiecePriorityNone)
			}
		}
	}
}

func (c *Cache) GetCapacity() int64 {
	if c == nil {
		return 0
	}
	return c.effectiveCapacity()
}
