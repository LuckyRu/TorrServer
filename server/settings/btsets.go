package settings

import (
	"encoding/json"
	"io"
	"io/fs"
	"path/filepath"
	"strings"

	"server/log"
)

type TorznabConfig struct {
	Host string
	Key  string
	Name string
}

type TMDBConfig struct {
	APIKey     string // TMDB API Key
	APIURL     string // Base API URL (default: https://api.themoviedb.org)
	ImageURL   string // Image URL (default: https://image.tmdb.org)
	ImageURLRu string // Image URL for Russian users (default: https://imagetmdb.com)
}

type BTSets struct {
	// Cache
	CacheSize       int64 // in byte, def 64 MB
	ReaderReadAHead int   // in percent, 5%-100%, [...S__X__E...] [S-E] not clean
	PreloadCache    int   // in percent

	// Disk
	UseDisk           bool
	TorrentsSavePath  string
	RemoveCacheOnDrop bool

	// Torrent
	ForceEncrypt             bool
	RetrackersMode           int  // 0 - don`t add, 1 - add retrackers (def), 2 - remove retrackers 3 - replace retrackers
	TorrentDisconnectTimeout int  // in seconds
	EnableDebug              bool // debug logs

	// DLNA
	EnableDLNA bool
	// Bonjour/mDNS LAN discovery (_torrserver, _http, _https)
	EnableBonjour bool
	// Shared name for DLNA and Bonjour
	FriendlyName string

	// Rutor
	EnableRutorSearch bool

	// Torznab
	EnableTorznabSearch bool
	TorznabUrls         []TorznabConfig

	// TMDB
	TMDBSettings TMDBConfig

	// BT Config
	EnableIPv6        bool
	DisableTCP        bool
	DisableUTP        bool
	DisableUPNP       bool
	DisableDHT        bool
	DisablePEX        bool
	DisableUpload     bool
	DownloadRateLimit int // in kb, 0 - inf
	UploadRateLimit   int // in kb, 0 - inf
	ConnectionsLimit  int
	PeersListenPort   int

	// LPD
	EnableLPD bool
	LPDIPv6   bool

	// HTTPS
	SslPort int
	SslCert string
	SslKey  string

	// Reader
	ResponsiveMode bool // enable Responsive reader (don't wait pieceComplete)

	// FS
	ShowFSActiveTorr bool

	// Storage preferences
	StoreSettingsInJson bool
	StoreViewedInJson   bool

	// Viewed timecodes
	TrackTimecode bool // store playback position (timecode) in viewed data
}

func (v *BTSets) String() string {
	buf, _ := json.Marshal(v)
	return string(buf)
}

var BTsets *BTSets

func SetBTSets(sets *BTSets) {
	if ReadOnly {
		return
	}
	sets.applyFailsafeDefaults()

	if sets.TorrentsSavePath == "" {
		sets.UseDisk = false
	} else if sets.UseDisk {
		BTsets = sets

		go filepath.WalkDir(sets.TorrentsSavePath, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() && strings.ToLower(d.Name()) == ".tsc" {
				BTsets.TorrentsSavePath = path
				log.TLogln("Find directory \"" + BTsets.TorrentsSavePath + "\", use as cache dir")
				return io.EOF
			}
			if d.IsDir() && strings.HasPrefix(d.Name(), ".") {
				return filepath.SkipDir
			}
			return nil
		})
	}

	BTsets = sets
	buf, err := json.Marshal(BTsets)
	if err != nil {
		log.TLogln("Error marshal btsets", err)
		return
	}
	tdb.Set("Settings", "BitTorr", buf)
}

// Defaults for a household rather than a single viewer.
//
// The upstream 64 MB and 25 connections are sized for one stream at a modest bitrate. A
// 4K stream at ~50 Mbps drains 64 MB in about ten seconds, and the cache is then split
// between everyone watching that torrent, so the numbers run out exactly when several
// people are watching.
//
// The cache goes to disk because at this size holding it in RAM is the wrong trade: disk
// is what there is a lot of, and the pieces are written once and read once.
//
// The preload percentage has to come down as the cache goes up, because it is a percentage
// of the cache and the viewer waits through all of it before playback may start. At the
// upstream 50% this cache would preload 128 MB, which took a minute and a half and kept the
// pipeline from prerolling for the whole of it. 12% lands back on the ~32 MB the upstream
// pair produced, which is the amount that demonstrably starts in time.
const (
	defaultCacheSize        = 256 * 1024 * 1024
	defaultConnectionsLimit = 60
	defaultPreloadCache     = 12
	defaultCacheDirName     = "cache"
)

// applyFailsafeDefaults fills in what a stored configuration left at zero, which is how a
// config written before a setting existed arrives here.
func (v *BTSets) applyFailsafeDefaults() {
	if v.CacheSize == 0 {
		v.CacheSize = defaultCacheSize
	}
	if v.ConnectionsLimit == 0 {
		v.ConnectionsLimit = defaultConnectionsLimit
	}
	if v.TorrentDisconnectTimeout == 0 {
		v.TorrentDisconnectTimeout = 30
	}

	if v.ReaderReadAHead < 5 {
		v.ReaderReadAHead = 5
	}
	if v.ReaderReadAHead > 100 {
		v.ReaderReadAHead = 100
	}

	if v.PreloadCache < 0 {
		v.PreloadCache = 0
	}
	if v.PreloadCache > 100 {
		v.PreloadCache = 100
	}
}

func SetDefaultConfig() {
	sets := new(BTSets)
	sets.CacheSize = defaultCacheSize
	sets.PreloadCache = defaultPreloadCache
	sets.ConnectionsLimit = defaultConnectionsLimit
	// Without a path UseDisk is silently forced off, so the two go together.
	if Path != "" {
		sets.TorrentsSavePath = filepath.Join(Path, defaultCacheDirName)
		sets.UseDisk = true
		// A disk cache that is never cleaned fills the disk: the files outlive the
		// torrent that created them.
		sets.RemoveCacheOnDrop = true
	}
	sets.RetrackersMode = 1
	sets.TorrentDisconnectTimeout = 30
	sets.ReaderReadAHead = 95 // 95%
	sets.ResponsiveMode = true
	sets.ShowFSActiveTorr = true
	sets.StoreSettingsInJson = true
	sets.EnableLPD = true
	sets.LPDIPv6 = false
	sets.EnableBonjour = true
	// Set default TMDB settings
	sets.TMDBSettings = TMDBConfig{
		APIKey:     "",
		APIURL:     "https://api.themoviedb.org",
		ImageURL:   "https://image.tmdb.org",
		ImageURLRu: "https://imagetmdb.com",
	}
	BTsets = sets
	if !ReadOnly {
		buf, err := json.Marshal(BTsets)
		if err != nil {
			log.TLogln("Error marshal btsets", err)
			return
		}
		tdb.Set("Settings", "BitTorr", buf)
	}
}

func loadBTSets() {
	buf := tdb.Get("Settings", "BitTorr")
	if len(buf) > 0 {
		err := json.Unmarshal(buf, &BTsets)
		if err == nil {
			if BTsets.ReaderReadAHead < 5 {
				BTsets.ReaderReadAHead = 5
			}
			// Set default TMDB settings if missing (for existing configs)
			if BTsets.TMDBSettings.APIURL == "" {
				BTsets.TMDBSettings = TMDBConfig{
					APIKey:     "",
					APIURL:     "https://api.themoviedb.org",
					ImageURL:   "https://image.tmdb.org",
					ImageURLRu: "https://imagetmdb.com",
				}
			}
			// Default Bonjour on for configs that predate the setting.
			var raw map[string]json.RawMessage
			if json.Unmarshal(buf, &raw) == nil {
				if _, ok := raw["EnableBonjour"]; !ok {
					BTsets.EnableBonjour = true
				}
			}
			return
		}
		log.TLogln("Error unmarshal btsets", err)
	}
	// initialize defaults on error
	SetDefaultConfig()
}
