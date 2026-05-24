//go:build windows

package main

import (
	"os/exec"
	"syscall"
)

// detachProcess 脱离终端，以后台模式运行
func detachProcess(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{
		CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP | 0x08000000, // DETACHED_PROCESS
		HideWindow:    true,
	}
}
