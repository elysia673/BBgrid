//go:build linux || darwin

package process

import "syscall"

func setSysProcAttr(cmd *syscall.SysProcAttr) {
	cmd.Setsid = true
}
