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

	"davidb.org/x/gack/resticcmd"
	"davidb.org/x/gack/zfs"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// resticCmd represents the restic command
var resticCmd = &cobra.Command{
	Use:   "restic",
	Short: "Perform any necessary restic backups",
	Long: `Perform any needed backups of snapshots using restic described in the
config file.`,
	Run: func(cmd *cobra.Command, args []string) {
		var config ResticConfig
		err := viper.UnmarshalKey("restic", &config)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		fmt.Printf("pretend: %t\n", resticOptions.Pretend)
		fmt.Printf("restic called: %v\n\n", &config)

		for i := range config.Volumes {
			fmt.Printf("Restic %q\n", config.Volumes[i].Name)
			err = config.Volumes[i].Sync()
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
		}
	},
}

type ResticOptions struct {
	Pretend bool
}

var resticOptions ResticOptions

type ResticConfig struct {
	Volumes []ResticVolume
}

type ResticVolume struct {
	Name         string
	Zfs          string
	Bind         string
	Repo         string
	Passwordfile string

	repo  *resticcmd.Repo
	mount string
}

func init() {
	RootCmd.AddCommand(resticCmd)

	resticCmd.Flags().BoolVarP(&resticOptions.Pretend, "pretend", "n", false,
		"show what would have been executed, but don't actually run")
	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// resticCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// resticCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

// Sync attempts to catch up on any backups that need to be done
// between snapshots and the restic volume.
func (rv *ResticVolume) Sync() error {
	path := zfs.ParsePath(rv.Zfs)
	ds, err := zfs.GetSnaps(path)
	if err != nil {
		return err
	}

	rv.mount, err = FindMount(rv.Zfs, "zfs")
	if err != nil {
		return err
	}

	fmt.Printf("%d zfs volumes\n", len(ds))
	fmt.Printf("%d zfs snapshots\n", len(ds[0].Snaps))

	// Get information on backups we've done.
	rv.repo = &resticcmd.Repo{
		Path:         rv.Repo,
		Passwordfile: rv.Passwordfile,
	}

	snaps, err := rv.repo.GetSnapshots()
	if err != nil {
		return err
	}
	fmt.Printf("%d restic snapshots\n", len(snaps))

	// Collect all of the tags that have been snapped (where the
	// backup volume matches the Bind).
	backedSnaps := make(map[string]bool)
	for _, snap := range snaps {
		if snap.HasPath(rv.Bind) {
			for _, t := range snap.Tags {
				backedSnaps[t] = true
			}
		}
	}

	// Go through each ZFS snapshot and determine if it needs to
	// be backed up.
	// TODO: Handle child volumes better.
	for _, snap := range ds[0].Snaps {
		if backedSnaps[snap] {
			continue
		}
		if !resticOptions.Pretend {
			err = rv.SyncSingle(snap)
			if err != nil {
				return err
			}
		}

		// Limit number run each time.
		// break
	}

	return nil
}

// SyncSingle synchronizes a single backup.
func (rv *ResticVolume) SyncSingle(snap string) error {
	fmt.Printf("Back up %q:%q to %q\n", rv.Zfs, snap, rv.Repo)

	stat := filepath.Join(rv.mount, ".zfs", "snapshot", snap)

	// It is important to stat within the snapshot so that the ZFS
	// automounter will mount it.  We can't use filepath.Join,
	// because it will ignore the addition of the ".".
	_, err := os.Lstat(stat + "/.")
	if err != nil {
		return err
	}

	// Bind the mount to the desired Bind directory.
	mount, err := NewBindMount(stat, rv.Bind)
	if err != nil {
		return err
	}
	defer mount.Close()

	return rv.repo.RunBackup(rv.Bind, []string{snap})
}
