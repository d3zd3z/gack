// ZFS Filesystem support

package zfs // import "davidb.org/x/gack/zfs"

import (
	"bufio"
	"bytes"
	"log"
	"os"
	"os/exec"
	"strings"
)

// A location where we can run zfs commands.
type Path interface {
	// The local path name of this Path.  This is the name of the
	// ZFS filesystem
	Name() string

	// Construct a command to run a zfs command on this path.
	Command(args ...string) *exec.Cmd
}

// A local ZFS path.  The name refers to a volume accessible locally.
// It just wraps a string with the local path name.
type LocalPath string

func (p LocalPath) Name() string {
	return string(p)
}

func (p LocalPath) Command(args ...string) *exec.Cmd {
	cmd := exec.Command("zfs", args...)
	cmd.Stderr = os.Stderr
	return cmd
}

// A remote ZFS path.  There is a host and a path involved.
type RemotePath struct {
	Host string
	Path string
}

func (p *RemotePath) Name() string {
	return p.Path
}

func (p *RemotePath) Command(args ...string) *exec.Cmd {
	largs := append([]string{p.Host, "zfs"}, args...)
	cmd := exec.Command("ssh", largs...)
	cmd.Stderr = os.Stderr
	return cmd
}

// Parse a user-specified zfs descriptor and return the proper path
// type.  If the path contains a ':' character, the left side will be
// the host, and the right the path of a remote zfs filesystem,
// otherwise the path will be considered local.
func ParsePath(text string) Path {
	fields := strings.SplitN(text, ":", 2)
	switch len(fields) {
	case 1:
		return LocalPath(fields[0])
	case 2:
		return &RemotePath{
			Host: fields[0],
			Path: fields[1],
		}
	default:
		panic("Unexpected path split result")
	}
}

// A single ZFS filesystem.
type DataSet struct {
	Path  Path
	Name  string
	Snaps []string
}

func GetSnaps(path Path) ([]*DataSet, error) {
	log.Printf("zfs.getSnaps: %q", path.Name())

	cmd := path.Command("list", "-H", "-t", "all", "-o", "name", "-r", path.Name())

	// It is easiest to just use Output, as the result will not be
	// large
	buf, err := cmd.Output()
	if err != nil {
		return nil, err
	}

	rd := bytes.NewReader(buf)
	sc := bufio.NewScanner(rd)

	ds := make([]*DataSet, 0)

	for sc.Scan() {
		line := sc.Text()
		// TODO: Handle bookmarks with '#' as well as '@' for
		// snapshot
		vols := strings.SplitN(line, "@", 2)

		if len(vols) == 1 {
			ds = append(ds, &DataSet{
				Path:  path,
				Name:  vols[0],
				Snaps: []string{},
			})
		} else {
			last := ds[len(ds)-1]
			if vols[0] != last.Name {
				panic("Output of `zfs list` has snapshot out of order")
			}
			last.Snaps = append(last.Snaps, vols[1])
		}
	}
	if sc.Err() != nil {
		return nil, sc.Err()
	}

	return ds, nil
}
