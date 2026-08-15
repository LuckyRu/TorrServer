package torrstor

import (
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anacrolix/torrent"

	"server/log"
	"server/settings"
)

type Reader struct {
	torrent.Reader
	file *torrent.File

	// offset, readahead and isUse are written by the goroutine serving this stream and read
	// by the cache maintenance that walks every reader of the torrent. Reader.mu orders the
	// serving side against itself; it does not reach the maintenance side, so these have to
	// carry their own synchronisation.
	offset    atomic.Int64
	readahead atomic.Int64
	isUse     atomic.Bool

	cache    *Cache
	isClosed atomic.Bool

	///Preload
	lastAccess atomic.Int64
	mu         sync.Mutex
}

// ErrTooManyReaders means the torrent already has as many readers as it can serve without
// the readers starving each other. It is a refusal, not a failure of the source: a queue
// would hold the HTTP connections open and only move the problem.
var ErrTooManyReaders = errors.New("too many concurrent readers on this torrent")

func newReader(file *torrent.File, cache *Cache) (*Reader, error) {
	r := new(Reader)
	r.file = file
	r.Reader = file.NewReader()

	r.cache = cache
	r.isUse.Store(true)
	r.SetReadahead(0)

	if err := cache.admitReader(r); err != nil {
		// The anacrolix reader is already open, and abandoning it would leave its piece
		// claims behind.
		_ = r.Reader.Close()
		return nil, err
	}
	return r, nil
}

func (r *Reader) Seek(offset int64, whence int) (n int64, err error) {
	if r.isClosed.Load() {
		return 0, io.EOF
	}
	switch whence {
	case io.SeekStart:
		r.offset.Store(offset)
	case io.SeekCurrent:
		r.offset.Add(offset)
	case io.SeekEnd:
		r.offset.Store(r.file.Length() + offset)
	}
	r.readerOn()
	n, err = r.Reader.Seek(offset, whence)
	r.offset.Store(n)
	r.lastAccess.Store(time.Now().Unix())
	return
}

func (r *Reader) Read(p []byte) (n int, err error) {
	err = io.EOF
	if r.isClosed.Load() {
		return
	}
	if r.file.Torrent() != nil && r.file.Torrent().Info() != nil {
		r.readerOn()
		n, err = r.Reader.Read(p)

		// samsung tv fix xvid/divx
		//if r.offset == 0 && len(p) >= 192 {
		//	str := strings.ToLower(string(p[112:116]))
		//	if str == "xvid" || str == "divx" {
		//		p[112] = 0x4D // M
		//		p[113] = 0x50 // P
		//		p[114] = 0x34 // 4
		//		p[115] = 0x56 // V
		//	}
		//	str = strings.ToLower(string(p[188:192]))
		//	if str == "xvid" || str == "divx" {
		//		p[188] = 0x4D // M
		//		p[189] = 0x50 // P
		//		p[190] = 0x34 // 4
		//		p[191] = 0x56 // V
		//	}
		//}

		r.offset.Add(int64(n))
		r.lastAccess.Store(time.Now().Unix())
	} else {
		log.TLogln("Torrent closed and readed")
	}
	return
}

func (r *Reader) SetReadahead(length int64) {
	if r.cache != nil && length > 0 {
		if capacity := r.cache.effectiveCapacity(); capacity > 0 && length > capacity {
			length = capacity
		}
	}
	if r.isUse.Load() {
		r.Reader.SetReadahead(length)
	}
	r.readahead.Store(length)
}

func (r *Reader) Offset() int64 {
	return r.offset.Load()
}

func (r *Reader) Readahead() int64 {
	return r.readahead.Load()
}

func (r *Reader) Close() {
	// file reader close in gotorrent
	// this struct close in cache
	r.isClosed.Store(true)
	if len(r.file.Torrent().Files()) > 0 {
		r.Reader.Close()
	}
	go r.cache.getRemPieces()
}

func (r *Reader) getPiecesRange() Range {
	startOff, endOff := r.getOffsetRange()
	return Range{r.getPieceNum(startOff), r.getPieceNum(endOff), r.file}
}

func (r *Reader) getReaderPiece() int {
	return r.getPieceNum(r.offset.Load())
}

func (r *Reader) getReaderRAHPiece() int {
	return r.getPieceNum(r.offset.Load() + r.readahead.Load())
}

func (r *Reader) getPieceNum(offset int64) int {
	return int((offset + r.file.Offset()) / r.cache.pieceLength)
}

func (r *Reader) getOffsetRange() (int64, int64) {
	prc := int64(settings.BTsets.ReaderReadAHead)
	readers := int64(r.getUseReaders())
	if readers == 0 {
		readers = 1
	}

	// Capacity scales with the reader count, so this division gives each viewer the window
	// a single viewer would have had rather than a shrinking share of one.
	window := r.cache.effectiveCapacity() / readers
	offset := r.offset.Load()
	beginOffset := offset - window*(100-prc)/100
	endOffset := offset + window*prc/100

	if beginOffset < 0 {
		beginOffset = 0
	}

	if endOffset > r.file.Length() {
		endOffset = r.file.Length()
	}
	return beginOffset, endOffset
}

func (r *Reader) checkReader() {
	if time.Now().Unix() > r.lastAccess.Load()+60 && r.cache.Readers() > 1 {
		r.readerOff()
	} else {
		r.readerOn()
	}
}

func (r *Reader) readerOn() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.isUse.Load() {
		if pos, err := r.Reader.Seek(0, io.SeekCurrent); err == nil && pos == 0 {
			r.Reader.Seek(r.offset.Load(), io.SeekStart)
		}
		r.isUse.Store(true)
		r.SetReadahead(r.readahead.Load())
	}
}

func (r *Reader) readerOff() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.isUse.Load() {
		r.SetReadahead(0)
		r.isUse.Store(false)
		if r.offset.Load() > 0 {
			r.Reader.Seek(0, io.SeekStart)
		}
	}
}

func (r *Reader) getUseReaders() int {
	return r.cache.GetUseReaders()
}
