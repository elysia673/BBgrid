//go:build linux || darwin

package daemon

import "syscall"

func setSysProcAttr(attr *syscall.SysProcAttr) {
	attr.Setsid = true
}
