package main

import (
	"fmt"
	"time"
)

func main() {
	t := time.Now()
	zone, offset := t.Zone()
	fmt.Println(zone, offset)

	ts := time.Unix(int64(offset*-1), 0)
	fmt.Printf("ts: %v\n", ts)
}
