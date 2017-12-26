package cmd

import (
	"os/exec"
)

// A BindMount makes one directory tree appear at the location of
// another.
type BindMount string

// NewBindMount attempts to mount the given source at the given dest.
// If the return is successful, the user should call Close to clean up
// the bind.
func NewBindMount(source, dest string) (BindMount, error) {
	err := exec.Command("mount", "--bind", source, dest).Run()
	if err != nil {
		return "", err
	}

	return BindMount(dest), nil
}

// Close unmounts a bind mount.
func (b BindMount) Close() error {
	return exec.Command("umount", string(b)).Run()
}
