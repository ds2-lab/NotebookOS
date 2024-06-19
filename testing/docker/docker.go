package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"time"
)

func main() {
	// errs := make(chan error, 1)
	ctx := context.Background()

	fd := func(proto, addr string) (conn net.Conn, err error) {
		return net.Dial("unix", "/var/run/docker.sock")
	}
	tr := &http.Transport{
		Dial: fd,
	}
	client := &http.Client{Transport: tr}

	filters := make(map[string]string)
	filters["type"] = "container"

	url := fmt.Sprintf("http://localhost/v1.45/events?filters={\"type\":[\"container\"],\"event\":[\"create\"]}")
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
			return
		default:
			fmt.Printf("Got output:\n")
			var event map[string]interface{}
			if err := decoder.Decode(&event); err != nil {
				time.Sleep(time.Millisecond * 10)
				continue
			}

			fmt.Printf("Decoded JSON:\n")
			for k, v := range event {
				fmt.Printf("\"%s\": \"%s\"\n", k, v)
			}
			fmt.Printf("\nContainer ID: %s\nShort ID: %s\nKernel ID: %s\n", event["id"], event["id"].(string)[0:12], event["Actor"].(map[string]interface{})["Attributes"].(map[string]interface{})["kernel_id"])
		}
	}
}
