package main

import (
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/mason-leap-lab/go-utils/config"

	"github.com/zhangjyr/distributed-notebook/gateway/daemon"
	"github.com/zhangjyr/distributed-notebook/gateway/domain"
)

var (
	options domain.ClusterGatewayOptions = domain.ClusterGatewayOptions{}
	logger                               = config.GetLogger("")
	sig                                  = make(chan os.Signal, 1)
)

func init() {
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM, syscall.SIGABRT)
	// Set default options.
	options.Port = 8080
	options.ProvisionerPort = 8081
	options.ConnectionInfo.Transport = "tcp"
}

// Create and run the debug HTTP server.
// We don't have any meaningful endpoints that we add directly.
// But we include the following import statement at the top of this file:
//
//	_ "net/http/pprof"
//
// This adds several key debug endpoints.
//
// Important: this should be called from its own goroutine.
func CreateAndStartDebugHttpServer() {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		log.Printf("Received HTTP debug connection to '/'")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(fmt.Sprintf("%d - Hello\n", http.StatusOK)))
	})

	http.HandleFunc("/test", func(w http.ResponseWriter, r *http.Request) {
		log.Printf("Received HTTP debug connection to '/test'")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(fmt.Sprintf("%d - Test\n", http.StatusOK)))
	})

	var address string = fmt.Sprintf(":%d", options.DebugPort)
	log.Printf("Serving debug HTTP server: %s\n", address)

	if err := http.ListenAndServe(address, nil); err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}

// Ensure that the options/configuration is valid.
func ValidateOptions() {
	flags, err := config.ValidateOptions(&options)
	if err == config.ErrPrintUsage {
		flags.PrintDefaults()
		os.Exit(0)
	} else if err != nil {
		log.Fatal(err)
	}
}

func main() {
	defer finalize(false, "Main thread", nil)

	var done sync.WaitGroup

	// Ensure that the options/configuration is valid.
	ValidateOptions()

	logger.Info("Starting Cluster Gateway with the following options: %v", options)

	if options.DebugMode {
		go CreateAndStartDebugHttpServer()
	}

	daemon.CreateAndStartClusterGatewayComponents(&options, &done, finalize, sig)

	done.Wait()
}

func finalize(fix bool, identity string, distributedCluster *daemon.DistributedCluster) {
	if !fix {
		return
	}

	if err := recover(); err != nil {
		logger.Error("%v", err)

		if distributedCluster != nil {
			distributedCluster.HandlePanic(identity, err)
		}
	}

	sig <- syscall.SIGINT
}
