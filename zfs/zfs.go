// ZFS Filesystem support

package zfs // import "davidb.org/x/gack/zfs"

import (
	"bufio"
	"bytes"
	"errors"
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
	Books []string
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
			vols = strings.SplitN(line, "#", 2)
			if len(vols) == 1 {
				ds = append(ds, &DataSet{
					Path: path,
					Name: vols[0],
				})
			} else {
				last := ds[len(ds)-1]
				if vols[0] != last.Name {
					panic("Output of `zfs list` has bookmark out of order")
				}
				last.Books = append(last.Books, vols[1])
			}
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

// Bookmark creates a bookmark of the same name from a given snapshot.
func (ds *DataSet) Bookmark(name string) error {
	var stderr bytes.Buffer

	cmd := ds.Path.Command("bookmark", ds.Name+"@"+name, "#"+name)
	cmd.Stderr = &stderr

	err := cmd.Run()
	if err == nil && stderr.Len() == 0 {
		return nil
	}

	// If there is an error, allow it if it is the 'bookmark
	// exists' error.  It is a little fragile to parse the stderr
	// of the command, so this is a place to look if this starts
	// failing.
	text := stderr.String()
	if strings.HasSuffix(text, "bookmark exists\n") {
		return nil
	}

	os.Stderr.WriteString(text)
	if err == nil {
		err = errors.New("Non-empty stderr from bookmark command")
	}
	return err
}

// RemoveSnap removes a snapshot.
func (ds *DataSet) RemoveSnap(name string) error {
	cmd := ds.Path.Command("destroy", ds.Name+"@"+name)
	return cmd.Run()
}
