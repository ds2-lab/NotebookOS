package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/colinmarc/hdfs/v2"
)

func main() {
	fmt.Printf("Hello, world.\n")

	args := os.Args[1:]

	var ip string
	var port string

	if len(args) == 0 {
		ip = "172.17.0.1"
		port = "9000"
	} else if len(args) == 1 {
		ip = args[0]
		port = "9000"
	} else if len(args) == 2 {
		ip = args[0]
		port = args[1]
	} else {
		panic(fmt.Sprintf("Unsupported number of args: %v", args))
	}

	hostname := fmt.Sprintf("%s:%s", ip, port)

	fmt.Printf("Using hostname: %s\n", hostname)

	client, err := hdfs.NewClient(hdfs.ClientOptions{
		Addresses: []string{hostname},
		User:      "jovyan",
		// UseDatanodeHostname: false,
		DatanodeDialFunc: func(ctx context.Context, network, address string) (net.Conn, error) {
			port := strings.Split(address, ":")[1]
			modifiedAddress := fmt.Sprintf("%s:%s", "172.17.0.1", port)
			fmt.Printf("Dialing. Original address \"%s\". Modified address: %s.\n", address, modifiedAddress)
			conn, err := (&net.Dialer{}).DialContext(ctx, network, modifiedAddress)
			if err != nil {
				return nil, err
			}

			return conn, nil
		},
	})
	if err != nil {
		panic(err)
	}

	fileInfo, err := client.Stat("/")
	if err != nil {
		panic(err)
	}

	fmt.Printf("Stat(\"/\"):\nName: %s\nSize: %d\nIsDir: %v\n", fileInfo.Name(), fileInfo.Size(), fileInfo.IsDir())

	err = client.Remove("/test.txt")
	if err != nil {
		fmt.Printf("Error when removing \"/test.txt\": %v\n", err)
	}

	err = client.CopyToRemote("test.txt", "/test.txt")
	if err != nil {
		fmt.Printf("Error when copying-to-remote file \"/test.txt\": %v\n", err)
	}

	data, err := client.ReadFile("/test.txt")
	if err != nil {
		fmt.Printf("Error reading file \"/test.txt\": %v\n", err)
	} else {
		str := string(data)
		fmt.Printf("Read file \"/test.txt\": \"%s\"\n", str)
	}
}
