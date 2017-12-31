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
	"regexp"
	"time"

	"davidb.org/x/gack/zfs"
	"davidb.org/x/gosure/status"
	"davidb.org/x/gosure/store"
	"davidb.org/x/gosure/sure"
	"davidb.org/x/gosure/weave"
	"github.com/spf13/cobra"
)

// sureCmd represents the sure command
var sureCmd = &cobra.Command{
	Use:   "sure",
	Short: "Update the sure databases",
	Long:  `Update the sure database associated with this backup convention.`,
	Run: func(cmd *cobra.Command, args []string) {
		config := &GackConfig.Sure

		for i := range config.Volumes {
			fmt.Printf("Sure update %q\n", config.Volumes[i].Name)
			err := config.Volumes[i].SureSync()
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
		}
	},
}

type SureConfig struct {
	Volumes []SureVolume
}

type SureVolume struct {
	Name       string
	Zfs        string
	Bind       string
	Sure       string
	Convention string

	mount string
}

func init() {
	RootCmd.AddCommand(sureCmd)
	sureCmd.Flags().BoolVarP(&pretend, "pretend", "n", false,
		"show what would have been executed, but don't actually run")
}

// SureSync updates the sure database with a scan of any volumes needed.
func (sv *SureVolume) SureSync() error {
	stats := status.NewManager()
	defer stats.Close()

	path := zfs.ParsePath(sv.Zfs)
	dss, err := zfs.GetSnaps(path)
	if err != nil {
		return err
	}

	sv.mount, err = FindMount(sv.Zfs, "zfs")
	if err != nil {
		return err
	}

	// Filter the snaps to those of the given convention.
	var snaps []string

	re := regexp.MustCompile("^" + regexp.QuoteMeta(sv.Convention) + `(\d|-)?`)

	for _, sn := range dss[0].Snaps {
		if re.MatchString(sn) {
			snaps = append(snaps, sn)
		}
	}

	var st store.Store

	err = st.Parse(sv.Sure)
	if err != nil {
		return err
	}

	host, err := os.Hostname()
	if err != nil {
		return err
	}

	st.Tags = map[string]string{
		"host":       host,
		"volume":     sv.Name,
		"convention": sv.Convention,
		"zfs":        sv.Zfs,
	}

	stats.Printf("%d snapshots to check\n", len(snaps))

	for _, sn := range snaps {
		// Set the tags and parameters, in case we do write.
		st.Name = sn

		// Read the header if there is one.  An error
		// indicates nothing has been captured yet.
		hdr, _ := st.ReadHeader()
		if hdr == nil {
			err = sv.Scan(false, &st, sn, stats)
			if err != nil {
				return err
			}
		} else {
			if sv.ContainsSnap(&st, hdr, sn) {
				continue
			}
			err = sv.Scan(true, &st, sn, stats)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// ContainsSnap inciates if this header contain the given snapshot?
func (sv *SureVolume) ContainsSnap(st *store.Store, hdr *weave.Header, snap string) bool {
	for _, d := range hdr.Deltas {
		if d.Name == snap && d.Tags["host"] == st.Tags["host"] && d.Tags["zfs"] == st.Tags["zfs"] {
			return true
		}
	}

	return false
}

// Scan performs an sure scan.  If 'update' is true, uses the data
// from the previous scan to speed up the hashing.  Otherwise, does a
// fresh scan.
func (sv *SureVolume) Scan(update bool, st *store.Store, snap string, stats *status.Manager) error {
	fmt.Printf("Scanning %q:%q to %q\n", sv.Zfs, snap, sv.Sure)

	if pretend {
		return nil
	}

	stat := filepath.Join(sv.mount, ".zfs", "snapshot", snap)

	// Stat within the snapshot for the ZFS automounter to mount
	// it.
	_, err := os.Lstat(stat + "/.")
	if err != nil {
		return err
	}

	var oldTree *sure.Tree
	if update {
		oldTree, err = st.ReadDat()
		if err != nil {
			return err
		}
	}

	meter := stats.Meter(250 * time.Millisecond)
	newTree, err := sure.ScanFs(stat, meter)
	meter.Close()
	if err != nil {
		return err
	}

	if update {
		sure.MigrateHashes(oldTree, newTree)
	}
	hashUpdate(newTree, stat, stats)

	err = st.Write(newTree)
	if err != nil {
		return err
	}

	return nil
}

func hashUpdate(tree *sure.Tree, dir string, stats *status.Manager) {
	est := tree.EstimateHashes()
	meter := stats.Meter(250 * time.Millisecond)
	prog := sure.NewProgress(est.Files, est.Bytes, meter)
	prog.Flush()
	tree.ComputeHashes(&prog, dir)
	meter.Close()
}
