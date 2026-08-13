//go:build gst

package gstreamer

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func testContext(t *testing.T, target string, remoteAddr string, userAgent string) *gin.Context {
	t.Helper()

	request := httptest.NewRequest(http.MethodGet, target, nil)
	request.RemoteAddr = remoteAddr
	if userAgent != "" {
		request.Header.Set("User-Agent", userAgent)
	}

	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = request
	return c
}

func TestClientIDPrefersExplicitIdentifier(t *testing.T) {
	explicit := clientID(testContext(t, "/gst/hash/master.m3u8?client=living-room-tv", "192.168.1.5:5000", "Lampa"))
	if explicit[:2] != "c:" {
		t.Fatalf("explicit identifier should be tagged c:, got %q", explicit)
	}

	// The same device identifier from a different address and agent stays the same client:
	// that is the whole reason an explicit identifier beats a fingerprint.
	moved := clientID(testContext(t, "/gst/hash/master.m3u8?client=living-room-tv", "10.0.0.9:41234", "webOS"))
	if moved != explicit {
		t.Fatalf("explicit identity must survive an address change: %q != %q", moved, explicit)
	}
}

func TestClientIDFallsBackToFingerprint(t *testing.T) {
	first := clientID(testContext(t, "/gst/hash/master.m3u8", "192.168.1.5:5000", "Lampa"))
	if first[:2] != "d:" {
		t.Fatalf("derived identity should be tagged d:, got %q", first)
	}
	if !isDerivedClientID(first) {
		t.Fatalf("isDerivedClientID should accept %q", first)
	}

	// A new source port is the same device; only host and agent may take part.
	samePort := clientID(testContext(t, "/gst/hash/master.m3u8", "192.168.1.5:5001", "Lampa"))
	if samePort != first {
		t.Fatalf("source port must not change the fingerprint: %q != %q", samePort, first)
	}

	otherDevice := clientID(testContext(t, "/gst/hash/master.m3u8", "192.168.1.6:5000", "Lampa"))
	if otherDevice == first {
		t.Fatal("different hosts must not collapse into one fingerprint")
	}

	otherAgent := clientID(testContext(t, "/gst/hash/master.m3u8", "192.168.1.5:5000", "Chrome"))
	if otherAgent == first {
		t.Fatal("different agents on one host must not collapse into one fingerprint")
	}
}

func TestClientIDWithoutRequest(t *testing.T) {
	if id := clientID(nil); !isDerivedClientID(id) {
		t.Fatalf("a missing request must still yield a usable identity, got %q", id)
	}
}

func TestSessionTokenIsDeterministic(t *testing.T) {
	first := sessionToken("c:abc", "3", 0)
	if first != sessionToken("c:abc", "3", 0) {
		t.Fatal("a repeated master request must resolve to the same session")
	}

	for _, other := range []string{
		sessionToken("c:abd", "3", 0),
		sessionToken("c:abc", "4", 0),
		sessionToken("c:abc", "3", 1),
	} {
		if other == first {
			t.Fatal("client, file and audio must all take part in the token")
		}
	}
}

func TestClientRegistryReportsFingerprintOncePerClient(t *testing.T) {
	var registry clientRegistry

	derived := derivedClientID("192.168.1.5", "Lampa")
	registry.note(derived, "hash", "3", 0)

	record := registry.records[derived]
	if record == nil || !record.derivedLogged {
		t.Fatal("running on a fingerprint must be recorded")
	}
	if record.last.key != "3\x000" {
		t.Fatalf("unexpected session key %q", record.last.key)
	}

	// The flag is the whole mechanism keeping this off every single request.
	registry.note(derived, "hash", "3", 0)
	if !registry.records[derived].derivedLogged {
		t.Fatal("the once-per-client flag must stay set")
	}
}

func TestClientRegistryDetectsInterleavedConsumers(t *testing.T) {
	var registry clientRegistry
	derived := derivedClientID("192.168.1.5", "Lampa")

	// A, B, A within the window: one viewer walks forward through episodes, two viewers
	// on one fingerprint take turns.
	registry.note(derived, "hash", "3", 0)
	registry.note(derived, "hash", "4", 0)
	if registry.records[derived].sharedLogged {
		t.Fatal("a plain episode switch must not be reported as a shared identity")
	}

	registry.note(derived, "hash", "3", 0)
	if !registry.records[derived].sharedLogged {
		t.Fatal("interleaved sessions on one fingerprint must be reported")
	}
}

func TestClientRegistryIgnoresInterleavingForExplicitClients(t *testing.T) {
	var registry clientRegistry
	explicit := "c:" + shortSum("living-room-tv")

	registry.note(explicit, "hash", "3", 0)
	registry.note(explicit, "hash", "4", 0)
	registry.note(explicit, "hash", "3", 0)

	record := registry.records[explicit]
	if record.derivedLogged || record.sharedLogged {
		t.Fatal("a client that identifies itself is not degraded, whatever it plays")
	}
}

func TestClientRegistryIgnoresSlowInterleaving(t *testing.T) {
	var registry clientRegistry
	derived := derivedClientID("192.168.1.5", "Lampa")

	registry.note(derived, "hash", "3", 0)
	registry.note(derived, "hash", "4", 0)
	registry.records[derived].last.at = time.Now().UTC().Add(-2 * clientOverlapWindow)
	registry.note(derived, "hash", "3", 0)

	if registry.records[derived].sharedLogged {
		t.Fatal("coming back to an earlier file much later is a viewer, not a second consumer")
	}
}

func TestClientRegistryCleanupDropsStaleRecords(t *testing.T) {
	var registry clientRegistry
	derived := derivedClientID("192.168.1.5", "Lampa")

	registry.note(derived, "hash", "3", 0)
	registry.cleanup(time.Now().UTC())
	if len(registry.records) != 1 {
		t.Fatal("a fresh record must survive cleanup")
	}

	registry.cleanup(time.Now().UTC().Add(2 * clientRecordTTL))
	if len(registry.records) != 0 {
		t.Fatal("a stale record must be dropped")
	}
}
