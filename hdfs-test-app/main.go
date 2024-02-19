package main

import (
	"fmt"
	"os"

	"github.com/colinmarc/hdfs"
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

	client, err := hdfs.New(hostname)
	if err != nil {
		panic(err)
	}

	file_info, err := client.Stat("/")
	if err != nil {
		panic(err)
	}

	fmt.Printf("Stat(\"/\"):\nName: %s\nSize: %d\nIsDir: %v\n", file_info.Name(), file_info.Size(), file_info.IsDir())

	summary, err := client.GetContentSummary("/")
	if err != nil {
		panic(err)
	}

	fmt.Printf("\n\nContent summary of \"/\": DirectoryCount: %v\n", summary.DirectoryCount())

	// infos, err := client.ReadDir("/")
	// if err != nil {
	// 	panic(err)
	// }

	// fmt.Printf("\n\nNumber of files within \"/\": %d\n\n", len(infos))

	// for _, info := range infos {
	// 	fmt.Printf("Stat(\"/\"):\nName: %s\nSize: %d\nIsDir: %v\n", info.Name(), info.Size(), info.IsDir())
	// }
}
