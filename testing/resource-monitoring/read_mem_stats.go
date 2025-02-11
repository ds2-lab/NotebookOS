package main

import (
	"fmt"
	"runtime"
	"time"
)

func main() {
	var startMemStats runtime.MemStats
	runtime.ReadMemStats(&startMemStats)
	startTime := time.Now()

	// Simulate some work
	for i := 0; i < 100000000; i++ {
		_ = i * i
	}

	var endMemStats runtime.MemStats
	runtime.ReadMemStats(&endMemStats)
	endTime := time.Now()

	cpuTime := time.Duration(endMemStats.Sys - startMemStats.Sys)
	elapsedTime := endTime.Sub(startTime)

	cpuUsage := float64(cpuTime) / float64(elapsedTime) * 100

	fmt.Printf("CPU Usage: %.2f%%\n", cpuUsage)
}
