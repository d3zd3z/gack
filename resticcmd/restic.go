package resticcmd // import "davidb.org/x/gack/resticcmd"
import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"time"
)

const resticCmd = "/home/davidb/go/bin/restic"

// The subset of information from the restic snapshots command we care
// about.
type Snapshot struct {
	Time  time.Time `json:"time"`
	Paths []string  `json:"paths"`
	Tags  []string  `json:"tags"`
	ID    string    `json:"short_id"`
}

// HasPath returns true if the specified path is one of the paths
// backed up in this snapshot.
func (s *Snapshot) HasPath(path string) bool {
	for _, p := range s.Paths {
		if p == path {
			return true
		}
	}

	return false
}

type Repo struct {
	Path         string
	Passwordfile string
}

// GetSnapshots runs restic to determine the available commands.
func (r *Repo) GetSnapshots() ([]*Snapshot, error) {
	out, err := exec.Command(resticCmd, "-r", r.Path, "-p",
		r.Passwordfile, "snapshots", "--json").Output()
	if err != nil {
		return nil, err
	}
	fmt.Printf("%d bytes of output\n", len(out))

	var snaps []*Snapshot
	err = json.Unmarshal(out, &snaps)
	if err != nil {
		return nil, err
	}

	return snaps, nil
}

// RunBackup requests a backup, of the given tags and mountpoint.
func (r *Repo) RunBackup(source string, tags []string) error {
	cmd := exec.Command(resticCmd, "-r", r.Path, "-p", r.Passwordfile,
		"backup", "--exclude-caches")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	for _, t := range tags {
		cmd.Args = append(cmd.Args, "--tag", t)
	}
	cmd.Args = append(cmd.Args, source)

	return cmd.Run()
}
