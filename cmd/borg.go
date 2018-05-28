// Copyright © 2017 David Brown <davidb@davidb.org>
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
	"fmt"
	"os"
	"path/filepath"

	"davidb.org/x/gack/borgcmd"
	"davidb.org/x/gack/zfs"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// borgCmd represents the borg command
var borgCmd = &cobra.Command{
	Use:   "borg",
	Short: "Perform any necessary borg backups",
	Long: `Perform any needed backups of snapshots using borg described in the
config file.`,
	Run: func(cmd *cobra.Command, args []string) {
		var config BorgConfig
		err := viper.UnmarshalKey("borg", &config)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		fmt.Printf("pretend: %t\n", borgOptions.Pretend)
		fmt.Printf("borg called: %v\n\n", &config)

		for i := range config.Volumes {
			fmt.Printf("Borg %q\n", config.Volumes[i].Name)
			err = config.Volumes[i].Sync()
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
		}
	},
}

type BorgOptions struct {
	Pretend bool
	Limit   int
}

var borgOptions BorgOptions

type BorgConfig struct {
	Volumes []BorgVolume
}

type BorgVolume struct {
	Name string
	Zfs  string
	Bind string
	Repo string

	repo  *borgcmd.Repo
	mount string
}

func init() {
	RootCmd.AddCommand(borgCmd)

	borgCmd.Flags().BoolVarP(&borgOptions.Pretend, "pretend", "n", false,
		"show what would have been executed, but don't actually run")

	borgCmd.Flags().IntVarP(&borgOptions.Limit, "limit", "l", 0,
		"Limit the total number of backups to be run.")
	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// resticCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// resticCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

var borgCount = 0

// Sync attempts to catch up on any backups that need to be done
// between snapshots and the borg volume.
func (bv *BorgVolume) Sync() error {
	path := zfs.ParsePath(bv.Zfs)
	ds, err := zfs.GetSnaps(path)
	if err != nil {
		return err
	}

	bv.mount, err = FindMount(bv.Zfs, "zfs")
	if err != nil {
		return err
	}

	fmt.Printf("%d zfs volumes\n", len(ds))
	fmt.Printf("%d zfs snapshots\n", len(ds[0].Snaps))

	bv.repo = &borgcmd.Repo{
		Path: bv.Repo,
	}

	snaps, err := bv.repo.GetSnapshots()
	if err != nil {
		return err
	}
	fmt.Printf("%d borg snapshots\n", len(snaps.Archives))

	// Collect all of the tags that have been captured by
	// snapshots.
	backedSnaps := make(map[string]bool)
	for _, snap := range snaps.Archives {
		name, tag, err := snap.GetName()
		if err != nil {
			// Just skip the ones that are invalid.
			continue
		}
		if bv.Name != name {
			continue
		}

		backedSnaps[tag] = true
	}

	total := 0
	for _, snap := range ds[0].Snaps {
		if !backedSnaps[snap] {
			total++
		}
	}

	fmt.Printf("%d snapshots to sync to borg\n", total)

	// Go through each ZFS snapshot and determine if it needs to
	// be backed up.
	// TODO: Handle child volumes better.
	i := 0
	for _, snap := range ds[0].Snaps {
		if backedSnaps[snap] {
			continue
		}

		i++
		fmt.Printf("-----------------------------------\n")
		fmt.Printf("Backing borg %d of %d\n", i, total)
		if !borgOptions.Pretend {
			err = bv.SyncSingle(snap)
			if err != nil {
				return err
			}
		} else {
			fmt.Printf("Would back up %q:%q to %q\n", bv.Zfs, snap, bv.Repo)
		}

		// Check the limit.  Note that we will always do at
		// least one from each volume.
		borgCount++
		if borgOptions.Limit > 0 && borgCount >= borgOptions.Limit {
			fmt.Printf("Reached limit, stopping\n")
			break
		}
	}

	return nil
}

func (bv *BorgVolume) SyncSingle(snap string) error {
	fmt.Printf("Back up %q:%q to %q\n", bv.Zfs, snap, bv.Repo)

	stat := filepath.Join(bv.mount, ".zfs", "snapshot", snap)

	// It is important to stat within the snapshot so that the ZFS
	// automounter will mount it.  We can't use filepath.Join,
	// because it will ignore the addition of the ".".
	_, err := os.Lstat(stat + "/.")
	if err != nil {
		return err
	}

	// Bind the mount to the desired Bind directory
	mount, err := NewBindMount(stat, bv.Bind)
	if err != nil {
		return err
	}
	defer mount.Close()

	return bv.repo.RunBackup(bv.Bind, bv.Name+"-"+snap)
}
