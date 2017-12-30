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
	"regexp"
	"time"

	"davidb.org/x/gack/zfs"
	"github.com/spf13/cobra"
)

// pruneCmd represents the prune command
var pruneCmd = &cobra.Command{
	Use:   "prune",
	Short: "Prune ",
	Long:  `Prune any expired backups based on the policy given.`,
	Run: func(cmd *cobra.Command, args []string) {
		snapPruneCmd(func(vol *SnapVolume, conv *SnapConvention) error {
			return vol.Prune(conv)
		})
	},
}

func init() {
	RootCmd.AddCommand(pruneCmd)
	pruneCmd.Flags().BoolVarP(&pretend, "pretend", "n", false,
		"show what would have been executed, but don't actually run")
}

func (v *SnapVolume) Prune(conv *SnapConvention) error {
	p := zfs.ParsePath(v.Zfs)

	dss, err := zfs.GetSnaps(p)
	if err != nil {
		return err
	}

	ds := dss[0]

	fmt.Printf("Need to look through %d snapshots\n", len(ds.Snaps))

	// Determine the local time to interpret these times
	// accordingly.  Unclear if this does the right thing with
	// DST.  (It does not, we really should use either UTC, or put
	// the TZ on the names).
	tz := time.Now().Format(" -0700 MST")

	// Go through each snapshot trying to decode a name from it
	// that matches the given pattern
	re := regexp.MustCompile("^" + regexp.QuoteMeta(conv.Name) + `(\d*)-(\d+)$`)

	var buckets = [6]struct {
		Count  int
		bucker func(d time.Time, nr int) int
		Last   int
	}{
		{conv.Last, always, -1},
		{conv.Hourly, ymdh, -1},
		{conv.Daily, ymd, -1},
		{conv.Weekly, yw, -1},
		{conv.Monthly, ym, -1},
		{conv.Yearly, y, -1},
	}

	var keeps []string
	var removes []string

	// The snapshots are returned in order, the pruning wants them
	// in the reverse order, so just build it that way.
	rsnaps := make([]string, 0, len(ds.Snaps))
	for i := len(ds.Snaps); i > 0; i-- {
		rsnaps = append(rsnaps, ds.Snaps[i-1])
	}

	for nr, sn := range rsnaps {
		var keepSnap bool

		m := re.FindStringSubmatch(sn)
		if m == nil {
			continue
		}

		// Assume the date is of this form.
		tm, err := time.Parse("200601021504 -0700 MST", m[2]+tz)
		if err != nil {
			fmt.Printf("Invalid time: %q (%s)\n", m[2], err)
			continue
		}

		// fmt.Printf("Time: %q, %s\n", sn, tm)

		// Update the buckets
		for i, b := range buckets {
			if b.Count > 0 {
				val := b.bucker(tm, nr)
				if val != b.Last {
					keepSnap = true
					buckets[i].Last = val
					buckets[i].Count--
				}
			}
		}

		if keepSnap {
			keeps = append(keeps, sn)
		} else {
			removes = append(removes, sn)
		}
	}

	reverseStrings(removes)

	fmt.Printf("Keep %d, prune %d\n", len(keeps), len(removes))

	if pretend {
		fmt.Printf("Would remove:\n")
		for _, s := range removes {
			fmt.Printf("    %s\n", s)
		}
	} else {
		for _, s := range removes {
			fmt.Printf("   Remove %s\n", s)
			err = ds.Bookmark(s)
			if err != nil {
				return err
			}

			err = ds.RemoveSnap(s)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// reverseStrings reverses a slice of strings.
func reverseStrings(ss []string) {
	last := len(ss) - 1
	for i := 0; i < len(ss)/2; i++ {
		ss[i], ss[last-i] = ss[last-i], ss[i]
	}
}

// This is borrowed from Restic, hopefully resulting in the same
// values.

/// ymdh returns an integer in the form YYYYMMDDHH.
func ymdh(d time.Time, _ int) int {
	return d.Year()*1000000 + int(d.Month())*10000 + d.Day()*100 + d.Hour()
}

// ymd returns an integer in the form YYYYMMDD.
func ymd(d time.Time, _ int) int {
	return d.Year()*10000 + int(d.Month())*100 + d.Day()
}

// yw returns an integer in the form YYYYWW, where WW is the week number.
func yw(d time.Time, _ int) int {
	year, week := d.ISOWeek()
	return year*100 + week
}

// ym returns an integer in the form YYYYMM.
func ym(d time.Time, _ int) int {
	return d.Year()*100 + int(d.Month())
}

// y returns the year of d.
func y(d time.Time, _ int) int {
	return d.Year()
}

// always returns a unique number for d.
func always(d time.Time, nr int) int {
	return nr
}
