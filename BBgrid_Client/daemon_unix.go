//go:build linux || darwin

package main

import (
	"os/exec"
	"syscall"
)

// detachProcess 脱离终端，以后台模式运行
func detachProcess(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setsid: true,
	}
}
