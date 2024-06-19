package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"
)

const (
	url string = "http://localhost/v1.45/events?filters={\"type\":[\"container\"],\"event\":[\"create\"],\"label\":[\"app=distributed_cluster\"]}"
)

func main() {
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		// errs := make(chan error, 1)
		ctx := context.Background()

		fd := func(ctx context.Context, proto, addr string) (conn net.Conn, err error) {
			return net.Dial("unix", "/var/run/docker.sock")
		}
		tr := &http.Transport{
			DialContext: fd,
		}
		client := &http.Client{Transport: tr}

		filters := make(map[string]string)
		filters["type"] = "container"

		// url := fmt.Sprintf(url, "distributed_cluster_default")
		fmt.Printf("URL: %s\n", url)
		resp, err := client.Get(url)
		if err != nil {
			panic(err)
		}

		decoder := json.NewDecoder(resp.Body)

		fmt.Printf("Reading output.\n")
		for {
			select {
			case <-ctx.Done():
				fmt.Printf("Done!\n")
				if err := ctx.Err(); err != nil {
					panic(err)
				}
				wg.Done()
				return
			default:
				fmt.Printf("Got output:\n")
				var containerCreationEvent map[string]interface{}
				if err := decoder.Decode(&containerCreationEvent); err != nil {
					time.Sleep(time.Millisecond * 10)
					continue
				}

				fullContainerId := containerCreationEvent["id"].(string)
				shortContainerId := fullContainerId[0:12]
				attributes := containerCreationEvent["Actor"].(map[string]interface{})["Attributes"].(map[string]interface{})

				var kernelId string
				if val, ok := attributes["kernel_id"]; ok {
					kernelId = val.(string)
				} else {
					fmt.Printf("Docker Container %s related to the distributed cluster has started.\n", shortContainerId)
					continue
				}

				fmt.Printf("Docker Container %s for kernel %s has started running.\n", shortContainerId, kernelId)
			}
		}

		wg.Done()
	}()

	wg.Wait()
}
