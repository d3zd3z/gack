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
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// snapCmd represents the snap command
var snapCmd = &cobra.Command{
	Use:   "snap",
	Short: "Perform any snapshots",
	Long:  `Perform any snapshots specified in the config file`,
	Run: func(cmd *cobra.Command, args []string) {
		// Make sure snapshots all have same time.
		now := time.Now()

		snapPruneCmd(func(vol *SnapVolume, conv *SnapConvention) error {
			return vol.Snap(now)
		})
	},
}

var (
	pretend bool
)

func snapPruneCmd(action func(vol *SnapVolume, conv *SnapConvention) error) {
	var config SnapConfig
	err := viper.UnmarshalKey("snap", &config)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	allConvs := make(map[string]*SnapConvention)
	for j := range config.Conventions {
		allConvs[config.Conventions[j].Name] = &config.Conventions[j]
	}

	for i := range config.Volumes {
		vol := &config.Volumes[i]
		// Find the convention.
		conv, ok := allConvs[vol.Convention]
		if !ok {
			fmt.Printf("Snap %q has unknown convention %q\n", vol.Name, vol.Convention)
			fmt.Printf("Skipping\n")
			continue
		}

		err = action(vol, conv)
		if err != nil {
			fmt.Printf("Error: %s\n", err)
			os.Exit(1)
		}
	}
}

type SnapConfig struct {
	Conventions []SnapConvention
	Volumes     []SnapVolume
}

type SnapConvention struct {
	Name    string
	Last    int
	Hourly  int
	Daily   int
	Weekly  int
	Monthly int
	Yearly  int
}

type SnapVolume struct {
	Name       string
	Convention string
	Zfs        string
}

func init() {
	RootCmd.AddCommand(snapCmd)
	snapCmd.Flags().BoolVarP(&pretend, "pretend", "n", false,
		"show what would have been executed, but don't actually run")
}

func (v *SnapVolume) Snap(now time.Time) error {
	name := fmt.Sprintf("%s-%s", v.Convention,
		now.Format("20060102150405"))
	fmt.Printf("Snapshot %s@%s\n", v.Zfs, name)

	return nil
}
