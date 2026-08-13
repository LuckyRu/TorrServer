package settings

import (
	"fmt"
	"sync/atomic"
	"testing"
)

type stubDB struct{}

func (s *stubDB) CloseDB()                             {}
func (s *stubDB) Get(xPath, name string) []byte        { return []byte("v") }
func (s *stubDB) Set(xPath, name string, value []byte) {}
func (s *stubDB) List(xPath string) []string           { return []string{"a", "b"} }
func (s *stubDB) Rem(xPath, name string)               {}
func (s *stubDB) Clear(xPath string)                   {}

// -race is the primary oracle. What still means something without it: List must return a slice
// the caller owns. Handing back the cache's own backing array lets a concurrent Set rewrite the
// names a caller is already iterating.
func TestDBReadCacheConcurrentSetList(t *testing.T) {
	cdb := NewDBReadCache(&stubDB{})

	const iters = 2000
	var aliased atomic.Int64

	startTogether(3, func(worker int) {
		for i := 0; i < iters; i++ {
			switch worker {
			case 0:
				cdb.Set("viewed", fmt.Sprintf("n%d", i), []byte("x"))
			case 1:
				cdb.Rem("viewed", fmt.Sprintf("n%d", i))
			default:
				listed := cdb.List("viewed")
				if len(listed) == 0 {
					continue
				}
				// Mutating the result must not be visible to the next caller.
				listed[0] = "\x00probe"
				if next := cdb.List("viewed"); len(next) > 0 && next[0] == "\x00probe" {
					aliased.Add(1)
				}
			}
		}
	})

	if got := aliased.Load(); got != 0 {
		t.Fatalf("List handed back its own backing array %d times", got)
	}
}
