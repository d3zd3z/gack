// Copyright © 2018 David Brown <davidb@davidb.org>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"regexp"

	"davidb.org/x/gack/zfs"
	"github.com/spf13/cobra"
)

// cloneCmd represents the clone command
var cloneCmd = &cobra.Command{
	Use:   "clone",
	Short: "Clone any volumes in config file",
	Long: `Reads the 'clone' block of the config file, syncing up any
filesystems mentioned there.`,
	Run: func(cmd *cobra.Command, args []string) {
		for _, v := range GackConfig.Clone.Volumes {
			fmt.Printf("Clone %#v\n", v)
			err := v.CloneSync()
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
		}
	},
}

type CloneConfig struct {
	Volumes []CloneVolume
}

type CloneVolume struct {
	Name   string
	Source string
	Dest   string
}

func init() {
	RootCmd.AddCommand(cloneCmd)
}

func (cv *CloneVolume) CloneSync() error {
	spath := zfs.ParsePath(cv.Source)
	slist, err := zfs.GetSnaps(spath)
	if err != nil {
		return err
	}

	dpath := zfs.ParsePath(cv.Dest)
	dlist, err := zfs.GetSnaps(dpath)
	if err != nil {
		return err
	}

	// All of the destinations, by short name.
	dests := make(map[string]*zfs.DataSet)
	for _, d := range dlist {
		sn, err := zfs.ShortName(dpath, d.Name)
		if err != nil {
			return err
		}
		dests[sn] = d
	}

	// Go through the sources, to see what needs to be cloned.
	for _, src := range slist {
		sn, err := zfs.ShortName(spath, src.Name)
		if err != nil {
			return err
		}

		dest, ok := dests[sn]
		if !ok {
			// The destination volume doesn't even exist,
			// synthesize one so that we can backup to it.
			dest = &zfs.DataSet{
				Path: dpath,
				Name: dlist[0].Name + sn,
			}
		}
		if len(dest.Snaps) == 0 {
			err = cv.FreshClone(src, dest)
			if err != nil {
				return err
			}
		}
		err = cv.UpdateClone(src, dest)
		if err != nil {
			return err
		}
	}

	return nil
}

// FreshClone performs an initial clone to where there is no
// destination filesystem.  ZFS send doesn't seem to be able to send
// the full list of incrementals on an initial scan, so only send the
// first one, and we will do an update after the first fresh clone.
func (cv *CloneVolume) FreshClone(src, dest *zfs.DataSet) error {
	fmt.Printf("Clone %q to %q\n", src.Name, dest.Name)

	if len(src.Snaps) == 0 {
		return fmt.Errorf("Source has no snapshots: %q", src.Path)
	}

	args := []string{src.Name + "@" + src.Snaps[0]}
	err := cv.RunClone(src, dest, args)
	if err != nil {
		return err
	}

	// Since we're expecting to run a regular clone after this,
	// indicate that the destination is here so we know where to
	// start the backup sequence from.
	dest.Snaps = append(dest.Snaps, src.Snaps[0])
	return nil
}

// UpdateClone performs an updating clone where the destination should
// have at least one filesystem.
func (cv *CloneVolume) UpdateClone(src, dest *zfs.DataSet) error {
	fmt.Printf("Clone %q to %q\n", src.Name, dest.Name)

	if len(dest.Snaps) == 0 {
		panic("Dest has no snapshots, should be fresh clone")
	}

	if len(src.Snaps) == 0 {
		return fmt.Errorf("Source has no snapshots: %q", src.Path)
	}

	lastDest := dest.Snaps[len(dest.Snaps)-1]
	lastSrc := src.Snaps[len(src.Snaps)-1]

	// If the latest at the dest matches the latest at the source,
	// there is nothing to do.
	if lastDest == lastSrc {
		fmt.Printf("   up to date\n")
		return nil
	}

	// Scan for a snapshot, and if not found, look for a bookmark
	// of the source name.
	var srcName string
	for _, s := range src.Snaps {
		if s == lastDest {
			srcName = "@" + s
			break
		}
	}
	if srcName == "" {
		for _, s := range src.Books {
			if s == lastDest {
				srcName = "#" + s
				break
			}
		}
	}

	if srcName == "" {
		return fmt.Errorf("Source has no snapshot or bookmark matching dest")
	}

	args := []string{"-I", srcName, src.Name + "@" + lastSrc}
	return cv.RunClone(src, dest, args)
	// fmt.Printf("Backup -I %s -> %s@%s to %s\n", srcName, src.Path, lastSrc, dest.Path)
}

var sizeRe = regexp.MustCompile(`(?m:^size\t(\d+)$)`)

// RunClone runs the actual clone.
func (cv *CloneVolume) RunClone(src, dest *zfs.DataSet, args []string) error {
	// TODO: The error handling here isn't really right, and we
	// should figure out what needs to be closed if things don't
	// start (the pipes will leak if the programs don't get
	// started that use them.
	allArgs := append([]string{"send", "-p", "-n", "-P"}, args...)
	cmd := src.Path.Command(allArgs...)
	var linebuf bytes.Buffer
	cmd.Stdout = &linebuf
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	if err != nil {
		return err
	}

	// Scan for "size\tnnnn\n" in the output.
	m := sizeRe.FindStringSubmatch(linebuf.String())
	if m == nil {
		return fmt.Errorf("zfs send output doesn't report size")
	}
	size := m[1]

	// Now build up the clone command.
	allArgs = append([]string{"send", "-p"}, args...)
	srcCmd := src.Path.Command(allArgs...)
	p1, err := srcCmd.StdoutPipe()
	if err != nil {
		return err
	}

	pvCmd := exec.Command("pv", "-s", size)
	pvCmd.Stderr = os.Stderr
	p2, err := pvCmd.StdoutPipe()
	if err != nil {
		return err
	}
	pvCmd.Stdin = p1

	allArgs = append([]string{"receive", "-vF", "-x", "mountpoint", dest.Name})
	destCmd := dest.Path.Command(allArgs...)
	destCmd.Stderr = os.Stderr
	destCmd.Stdout = os.Stdout
	destCmd.Stdin = p2

	err1 := srcCmd.Start()
	if err1 != nil {
		return err1
	}
	err2 := pvCmd.Start()
	if err2 != nil {
		return err2
	}
	err3 := destCmd.Start()
	if err3 != nil {
		return err3
	}

	err2 = pvCmd.Wait()
	if err2 != nil {
		return err2
	}
	err1 = srcCmd.Wait()
	if err1 != nil {
		return err1
	}
	err3 = destCmd.Wait()
	if err3 != nil {
		return err3
	}

	return err
}
