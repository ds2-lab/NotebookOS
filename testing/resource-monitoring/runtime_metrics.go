package main

import (
	"fmt"
	"runtime/metrics"
	"time"
)

func main() {
	// Collect metrics
	metrics.All()
	m := metrics.All()
	for _, v := range m {
		if v.Name == "/cpu/classes/gc:cpu-seconds" {
			var sample metrics.Sample
			metrics.Read([]metrics.Sample{sample})
			fmt.Println(sample)
		}
	}
	time.Sleep(5 * time.Second)
	metrics.All()
	m = metrics.All()
	for _, v := range m {
		if v.Name == "/cpu/classes/gc:cpu-seconds" {
			var sample metrics.Sample
			metrics.Read([]metrics.Sample{sample})
			fmt.Println(sample)
		}
	}
}
