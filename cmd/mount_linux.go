package cmd

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
)

// FindMount determines where, if known, a volume of a given type is mounted.
// Linux mountpoints are returned in "/proc/mounts".  This makes some
// assumptions about mountpoints not having spaces, which is not
// necessarily true with the automounter involved.
func FindMount(name, kind string) (string, error) {
	file, err := os.Open("/proc/mounts")
	if err != nil {
		return "", err
	}
	defer file.Close()

	buf := bufio.NewReader(file)

	for {
		line, err := buf.ReadString('\n')
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}

		fields := strings.Split(line, " ")
		if len(fields) < 3 {
			continue
		}

		if fields[0] == name && fields[2] == kind {
			return fields[1], nil
		}
	}

	return "", fmt.Errorf("Unable to find mountpoint for %q of type %q", name, kind)
}
