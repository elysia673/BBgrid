package main

import (
	"BBgrid/BBgrid_Ctl/internal/client"
	"BBgrid/BBgrid_Ctl/internal/config"
	"fmt"
	"os"
)

func handleStart(socketPath string, args []string) {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "Error: target required (server|client|all)")
		os.Exit(1)
	}

	daemonClient := client.New(socketPath)
	target := args[0]

	switch target {
	case "server", "client":
		if err := daemonClient.SendCommand("start", target); err != nil {
			fmt.Fprintf(os.Stderr, "Start %s failed: %v\n", target, err)
			os.Exit(1)
		}
		fmt.Printf("%s started\n", target)
	case "all":
		daemonClient.SendCommand("start", "server")
		daemonClient.SendCommand("start", "client")
		fmt.Println("All services started")
	default:
		fmt.Fprintf(os.Stderr, "Unknown target: %s (use server|client|all)\n", target)
		os.Exit(1)
	}
}

func handleStop(socketPath string, args []string) {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "Error: target required (server|client|all)")
		os.Exit(1)
	}

	daemonClient := client.New(socketPath)
	target := args[0]

	switch target {
	case "server", "client":
		if err := daemonClient.SendCommand("stop", target); err != nil {
			fmt.Fprintf(os.Stderr, "Stop %s failed: %v\n", target, err)
			os.Exit(1)
		}
		fmt.Printf("%s stopped\n", target)
	case "all":
		daemonClient.SendCommand("stop", "server")
		daemonClient.SendCommand("stop", "client")
		fmt.Println("All services stopped")
	default:
		fmt.Fprintf(os.Stderr, "Unknown target: %s (use server|client|all)\n", target)
		os.Exit(1)
	}
}

func handleRestart(socketPath string, args []string) {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "Error: target required (server|client|all)")
		os.Exit(1)
	}

	daemonClient := client.New(socketPath)
	target := args[0]

	switch target {
	case "server", "client":
		if err := daemonClient.SendCommand("restart", target); err != nil {
			fmt.Fprintf(os.Stderr, "Restart %s failed: %v\n", target, err)
			os.Exit(1)
		}
		fmt.Printf("%s restarted\n", target)
	case "all":
		daemonClient.SendCommand("restart", "server")
		daemonClient.SendCommand("restart", "client")
		fmt.Println("All services restarted")
	default:
		fmt.Fprintf(os.Stderr, "Unknown target: %s (use server|client|all)\n", target)
		os.Exit(1)
	}
}

func handleStatus(socketPath string, args []string) {
	daemonClient := client.New(socketPath)

	status, err := daemonClient.GetStatus()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Get status failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Service Status:")
	fmt.Println("─────────────────────────────────────")
	for name, s := range status {
		state := "stopped"
		pid := "-"
		version := "-"
		if s.Running {
			state = "running"
			pid = fmt.Sprintf("%d", s.PID)
			version = s.Version
		}
		fmt.Printf("  %-12s  %-8s  PID: %-8s  Version: %s\n", name, state, pid, version)
	}
}

func handleUpdate(socketPath string, args []string) {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "Error: target required (server|client|all)")
		os.Exit(1)
	}

	fmt.Println("Update not implemented yet")
}

func handleRollback(socketPath string, args []string) {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "Error: target required (server|client|all)")
		os.Exit(1)
	}

	fmt.Println("Rollback not implemented yet")
}

func handleInit(args []string) {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "Error: target required (server|client|all)")
		os.Exit(1)
	}

	cm := config.NewManager()
	target := args[0]

	if err := cm.Init(target); err != nil {
		fmt.Fprintf(os.Stderr, "Init failed: %v\n", err)
		os.Exit(1)
	}
}
