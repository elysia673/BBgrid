//go:build windows

package process

import "syscall"

func setSysProcAttr(cmd *syscall.SysProcAttr) {
	cmd.CreationFlags = syscall.CREATE_NEW_PROCESS_GROUP | 0x08000000 // DETACHED_PROCESS
	cmd.HideWindow = true
}
