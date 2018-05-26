package borgcmd // import "davidb.org/x/gack/borgcmd"

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

const borgCmd = "/usr/bin/borg"

type Listing struct {
	Archives []*Archive `json:"archives"`
}

type Archive struct {
	Archive  string `json:"archive"`
	Barchive string `json:"barchive"`
	Id       string `json:"id"`
	Name     string `json:"name"`
	Start    string `json:"start"`
	Time     string `json:"time"`
}

func (a *Archive) GetName() (string, string, error) {
	fields := strings.SplitN(a.Name, "-", 2)
	if len(fields) != 2 {
		return "", "", errors.New("Invalid archive name")
	}

	return fields[0], fields[1], nil
}

type Repo struct {
	Path string
}

// GetSnapshots runs borg to determine the available snapshots.
func (r *Repo) GetSnapshots() (*Listing, error) {
	cmd := exec.Command(borgCmd, "list", "--json", r.Path)
	cmd.Stderr = os.Stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, err
	}
	fmt.Printf("%d bytes of output\n", len(out))

	var listing Listing
	err = json.Unmarshal(out, &listing)
	if err != nil {
		return nil, err
	}

	return &listing, nil
}

func (r *Repo) RunBackup(dir string, name string) error {
	cmd := exec.Command(borgCmd, "create", "-s", "--progress",
		"--one-file-system",
		"--exclude-caches",
		"--compression=lz4",
		fmt.Sprintf("%s::%s", r.Path, name),
		dir)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	return cmd.Run()
}
