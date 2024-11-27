package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
)

func Check(port int) (status bool, err error) {

	// Concatenate a colon and the port
	host := "0.0.0.0:" + strconv.Itoa(port)

	// Try to create a server with the port
	server, err := net.Listen("tcp", host)

	// if it fails then the port is likely taken
	if err != nil {
		return false, err
	}

	// close the server
	server.Close()

	// we successfully used and closed the port
	// so it's now available to be used again
	return true, nil

}

func main() {
	argsWithoutProg := os.Args[1:]

	port, err := strconv.Atoi(argsWithoutProg[0])
	if err != nil {
		panic(err)
	}

	status, err := Check(port)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Status: %v\n", status)
}
