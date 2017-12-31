package main

import (
	"os"
	"os/exec"
	"os/user"
	"path"
	"testing"
	"time"

	"davidb.org/x/gack/cmd"
)

// Testing of gack.
// This is a bit hard to test because it requires a lot of
// infrastructure to support.  We can make the test work by giving
// this a tree of zfs volumes it can use to do the testing.
// To avoid building tests as root, some of the commands will be run
// with sudo.

const testBase = "lint/gacktest"

type tstate struct {
	t        *testing.T
	username string
}

func TestGack(t *testing.T) {
	user, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}

	ts := tstate{
		t:        t,
		username: user.Username,
	}

	ts.makeTree()
}

// makeTree constructs the test tree in ZFS.
func (ts *tstate) makeTree() {
	// ts.run("zfs", "destroy", "-r", testBase)
	ts.run("sudo", "zfs", "create", testBase)
	defer func() {
		ts.run("sudo", "zfs", "destroy", "-r", testBase)
	}()
	ts.run("sudo", "zfs", "create", path.Join(testBase, "fs"))
	ts.run("sudo", "chown", "-R", ts.username, "/"+testBase)
	ts.run("cp", "-r", ".", "/"+testBase+"/fs")

	now := time.Now()
	err := snapconf.Volumes[0].Snap(now)
	if err != nil {
		ts.t.Fatal(err)
	}

	// And make a change
	err = os.Remove(path.Join("/"+testBase, "fs", "LICENSE"))
	if err != nil {
		ts.t.Fatal(err)
	}

	now = now.Add(5 * time.Second)
	err = snapconf.Volumes[0].Snap(now)
	if err != nil {
		ts.t.Fatal(err)
	}

	ts.t.Logf("Gosuring")
	err = sureconf.Volumes[0].SureSync()
	if err != nil {
		ts.t.Fatal(err)
	}

	// Assume gosure is in the path.
	c := exec.Command("gosure", "signoff", "-f", path.Join("/"+testBase, "fs-sure.dat.gz"))
	c.Stderr = os.Stderr
	out, err := c.Output()
	if err != nil {
		ts.t.Fatal(err)
	}

	if string(out) != "- file                   LICENSE\n" {
		ts.t.Fatalf("Unexpected gosure signoff output: %q\n", string(out))
	}
}

var snapconf = cmd.SnapConfig{
	Conventions: []cmd.SnapConvention{
		{
			Name: "caa",
		},
	},
	Volumes: []cmd.SnapVolume{
		{
			Name:       "fs",
			Convention: "caa",
			Zfs:        path.Join(testBase, "fs"),
		},
	},
}

var sureconf = cmd.SureConfig{
	Volumes: []cmd.SureVolume{
		{
			Name:       "fs",
			Zfs:        path.Join(testBase, "fs"),
			Bind:       "/mnt/tmp",
			Sure:       path.Join("/"+testBase, "fs-sure.dat.gz"),
			Convention: "caa",
		},
	},
}

func (ts *tstate) run(prog string, args ...string) {
	cmd := exec.Command(prog, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	if err != nil {
		ts.t.Fatal(err)
	}
}
