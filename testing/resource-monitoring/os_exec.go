package main

import (
	"fmt"
	"os/exec"
	"runtime"
	"strings"
)

func main() {
	var cmd *exec.Cmd
	if runtime.GOOS == "windows" {
		cmd = exec.Command("wmic", "cpu", "get", "loadpercentage", "/value")
	} else {
		cmd = exec.Command("top", "-bn1")
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	if runtime.GOOS == "windows" {
		output := string(out)
		parts := strings.Split(output, "=")
		if len(parts) > 1 {
			cpuUsage := strings.TrimSpace(parts[1])
			fmt.Println("CPU Usage:", cpuUsage)
		} else {
			fmt.Println("Could not parse CPU usage")
		}
	} else {
		output := string(out)
		lines := strings.Split(output, "\n")
		for _, line := range lines {
			if strings.Contains(line, "Cpu(s)") {
				parts := strings.Fields(line)
				cpuUsage := parts[1]
				fmt.Println("CPU Usage:", cpuUsage)
				break
			}
		}
	}
}
