//go:build gst

package gstreamer

import (
	"crypto/sha256"
	"encoding/hex"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

const (
	// Two different files requested in turn within this window mean the identity covers
	// more than one consumer; a wider gap is just somebody switching episodes.
	clientOverlapWindow = 30 * time.Second

	clientRecordTTL = 30 * time.Minute
)

// clientID returns a stable identity for whoever is asking.
//
// An explicit device identifier is the only reliable source: it survives an address
// change and tells two applications on one device apart. The address and agent
// fingerprint is the fallback for clients that send nothing, which on a home network
// separates the television from the phone well enough to be useful.
func clientID(c *gin.Context) string {
	if c == nil || c.Request == nil {
		return derivedClientID("", "")
	}
	if explicit := strings.TrimSpace(c.Query("client")); explicit != "" {
		return "c:" + shortSum(explicit)
	}

	host, _, err := net.SplitHostPort(c.Request.RemoteAddr)
	if err != nil {
		host = c.Request.RemoteAddr
	}
	return derivedClientID(host, c.Request.UserAgent())
}

func derivedClientID(host string, agent string) string {
	return "d:" + shortSum(host+"\x00"+agent)
}

func isDerivedClientID(client string) bool {
	return strings.HasPrefix(client, "d:")
}

// sessionToken is deterministic on purpose: a player re-requests master.m3u8 after every
// error, and a random token would hand it a fresh pipeline each time.
func sessionToken(client string, fileID string, audio int) string {
	return shortSum(client + "\x00" + fileID + "\x00" + strconv.Itoa(audio))
}

func shortSum(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:8])
}

// clientRegistry exists for observability only.
//
// Identity is an optimisation contract, not a precondition: a client that sends no
// identifier has to keep working, just with weaker isolation. That "weaker" has to be
// visible in the log, otherwise two devices sharing one fingerprint looks like a
// mysterious playback bug instead of a missing identifier.
type clientRegistry struct {
	mu      sync.Mutex
	records map[string]*clientRecord
}

type clientRecord struct {
	lastSeen      time.Time
	derivedLogged bool
	sharedLogged  bool
	last          clientSession
	prev          clientSession
}

type clientSession struct {
	key string
	at  time.Time
}

// note records a session mint and reports degradation once per client, not per request.
func (r *clientRegistry) note(client string, hash string, fileID string, audio int) {
	if client == "" {
		return
	}

	now := time.Now().UTC()
	key := fileID + "\x00" + strconv.Itoa(audio)
	derived := isDerivedClientID(client)

	r.mu.Lock()
	if r.records == nil {
		r.records = make(map[string]*clientRecord)
	}
	record := r.records[client]
	if record == nil {
		record = &clientRecord{}
		r.records[client] = record
	}
	record.lastSeen = now

	logDerived := derived && !record.derivedLogged
	record.derivedLogged = record.derivedLogged || derived

	logShared := false
	switch {
	case record.last.key == "":
		record.last = clientSession{key: key, at: now}
	case record.last.key == key:
		record.last.at = now
	default:
		// Coming back to the file that was playing before the current one, quickly: one
		// consumer moves forward through episodes, two consumers interleave.
		if derived && !record.sharedLogged &&
			record.prev.key == key && now.Sub(record.last.at) < clientOverlapWindow {
			record.sharedLogged = true
			logShared = true
		}
		record.prev = record.last
		record.last = clientSession{key: key, at: now}
	}
	r.mu.Unlock()

	if logDerived {
		gstDebugf("client=%s hash=%s identified by address fingerprint; pass client=<device id> to keep sessions apart", client, hash)
	}
	if logShared {
		gstWarnf("client=%s hash=%s one fingerprint serves more than one consumer; they will share a pipeline whenever they open the same file — pass client=<device id>", client, hash)
	}
}

func (r *clientRegistry) cleanup(now time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for client, record := range r.records {
		if now.Sub(record.lastSeen) > clientRecordTTL {
			delete(r.records, client)
		}
	}
}
