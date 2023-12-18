package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"unsafe"

	"github.com/google/uuid"
	"github.com/zhangjyr/distributed-notebook/smr"
)

const store = "store"
const wait = true

type Counter struct {
	Message string `json:"message"`
	Num     int    `json:"num"`
	Id      string
}

func main() {
	n := 3
	port := 19800
	peers := make([]string, n)
	for i := 0; i < n; i++ {
		peers[i] = fmt.Sprintf("http://127.0.0.1:%d", port+i)
	}

	counter := Counter{
		Message: "initial",
		Num:     0,
	}
	var committed chan string

	config := smr.NewConfig().WithChangeCallback(func(reader smr.ReadCloser, len int, id string) string {
		var diff Counter

		buff := make([]byte, len)
		val := smr.NewBytes(unsafe.Pointer(&buff), len)
		reader.Read(*val)

		json.Unmarshal(buff, &diff)
		counter.Num += diff.Num
		counter.Message = diff.Message
		log.Printf("In change callback, got %v", counter)
		if wait && committed != nil {
			committed <- diff.Id
		}
		return ""
	}).WithRestoreCallback(func(reader smr.ReadCloser, len int) string {
		buff := make([]byte, len)
		val := smr.NewBytes(unsafe.Pointer(&buff), len)
		reader.Read(*val)

		json.Unmarshal(buff, &counter)
		log.Printf("In restore callback, got %v", counter)
		return ""
	}).WithShouldSnapshotCallback(func(node *smr.LogNode) bool {
		shouldSnap := node.NumChanges() == 3
		log.Printf("In should snapshot callback, changed %d, will snapshot: %v", node.NumChanges(), shouldSnap)
		return shouldSnap
	}).WithSnapshotCallback(func(writer smr.WriteCloser) string {
		log.Println("Writing snapshot...")
		snap := Counter{
			Message: "Snapshot",
			Num:     counter.Num,
		}
		val, _ := json.Marshal(&snap)
		writer.Write(*smr.NewBytes(unsafe.Pointer(&val), len(val)))
		writer.Close()
		return ""
	})

	configSlave := smr.NewConfig().WithChangeCallback(func(reader smr.ReadCloser, len int, id string) string {
		var cnt Counter

		buff := make([]byte, len)
		val := smr.NewBytes(unsafe.Pointer(&buff), len)
		reader.Read(*val)

		json.Unmarshal(buff, &cnt)
		log.Printf("In change callback of slavers, got %v", cnt)
		return ""
	}).WithShouldSnapshotCallback(func(node *smr.LogNode) bool {
		// Disable snapshot on slaver.
		return false
	})

	_, err := os.Stat(store)
	if err != nil {
		if err := os.Mkdir(store, 0750); err != nil {
			log.Fatalf("Cannot create storage directory (%v)", err)
		}
	}

	nodes := make([]*smr.LogNode, n)
	for i := 0; i < n; i++ {
		nodes[i] = smr.NewLogNode(store, i+1, peers, false)
		if i == 0 {
			nodes[i].Start(config)
		} else {
			nodes[i].Start(configSlave)
		}
	}

	add1 := Counter{
		Message: "Add 1",
		Num:     1,
		Id:      uuid.New().String(),
	}
	val, _ := json.Marshal(&add1)
	if wait {
		committed = make(chan string)
		log.Printf("Add 1: %s", add1.Id)
	} else {
		log.Printf("Add 1")
	}
	nodes[1].Propose(*smr.NewBytes(unsafe.Pointer(&val), len(val)), nil, "Num")
	if wait {
		for id := <-committed; id != add1.Id; id = <-committed {
			log.Printf("Ignore: %s", id)
		}
	}

	for i := 0; i < n; i++ {
		nodes[i].Close()
	}

	log.Printf("Exiting state: %v, changes: %d", counter, nodes[0].NumChanges())
}
