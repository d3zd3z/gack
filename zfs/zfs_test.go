package zfs_test

import (
	"testing"

	"davidb.org/x/gack/zfs"

	"github.com/davecgh/go-spew/spew"
)

// It is a little hard to test this without knowing something about
// the local system.  Right now, this is just hardcoded for a zfs
// pool that I have on my machine

func TestHelloWorld(t *testing.T) {
	snaps, err := zfs.GetSnaps(zfs.ParsePath("lint"))
	if err != nil {
		t.Fatal(err)
	}

	spew.Dump(snaps)
}
