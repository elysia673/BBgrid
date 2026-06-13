//go:build windows

package daemon

import "syscall"

func setSysProcAttr(attr *syscall.SysProcAttr) {
	attr.CreationFlags = syscall.CREATE_NEW_PROCESS_GROUP | 0x08000000
	attr.HideWindow = true
}
