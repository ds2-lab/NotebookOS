package main

import (
	"fmt"
	"github.com/scusemua/distributed-notebook/smr"
	"sync"
	"time"
)

func run(id int, peerIDs []int, peerAddresses []string, wg *sync.WaitGroup) {
	defer wg.Done()

	node := smr.NewLogNode("/tmp/71973e49-1996-43b1-83aa-2362e74bb187", id,
		"distributed-notebook-storage", "s3",
		false, peerAddresses, peerIDs, false, -1, "docker")

	conf := smr.NewConfig()
	conf.ElectionTick = 10
	conf.HeartbeatTick = 1
	conf.Debug = true

	conf = conf.WithChangeCallback(func(closer smr.ReadCloser, i int, s string) string {
		fmt.Printf("WithChangeCallback[i=%d, s=\"%s\"]\n", i, s)
		return ""
	}).WithRestoreCallback(func(closer smr.ReadCloser, i int) string {
		fmt.Printf("WithRestoreCallback[i=%d]\n", i)
		return ""
	}).WithShouldSnapshotCallback(func(n *smr.LogNode) bool {
		fmt.Printf("WithRestoreCallback[node=%v]\n", n)

		return false
	}).WithSnapshotCallback(func(closer smr.WriteCloser) string {
		fmt.Println("WithSnapshotCallback")

		return ""
	})

	started := node.Start(conf)

	fmt.Printf("Started %v\n", started)
}

func main() {
	peerIDs := []int{1, 2, 3}
	peerAddresses := []string{"http://localhost:8080", "http://localhost:8081", "http://localhost:8082"}

	var wg sync.WaitGroup

	for i := 1; i < 4; i++ {
		wg.Add(1)
		go run(i, peerIDs, peerAddresses, &wg)
	}

	wg.Wait()
	time.Sleep(5 * time.Second)
}
