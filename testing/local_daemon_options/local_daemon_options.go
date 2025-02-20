package main

import (
	"fmt"
	"github.com/scusemua/distributed-notebook/local_daemon/domain"
	"gopkg.in/yaml.v3"
	"os"
)

func main() {
	f, err := os.Open("/home/scusemua/go/pkg/distributed-notebook/setup/ansible/roles/deploy-distr-notebook-docker-stack/files/local_daemon/daemon.yml")
	if err != nil {
		panic(err)
	}

	decoder := yaml.NewDecoder(f)

	var opts *domain.LocalDaemonOptions
	err = decoder.Decode(&opts)

	if err != nil {
		panic(err)
	}

	fmt.Printf("Local Daemon ClusterGatewayOptions:\n%s\n", opts.PrettyString(2))
}
