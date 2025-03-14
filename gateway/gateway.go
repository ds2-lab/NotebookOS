package main

import (
	"encoding/gob"
	"errors"
	"fmt"
	"github.com/scusemua/distributed-notebook/common/metrics"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime/debug"
	"runtime/pprof"
	"sync"
	"syscall"
	"time"

	"github.com/Scusemua/go-utils/config"
	"github.com/charmbracelet/lipgloss"
	"github.com/muesli/termenv"

	"github.com/scusemua/distributed-notebook/gateway/daemon"
	"github.com/scusemua/distributed-notebook/gateway/domain"
)

var (
	options = domain.ClusterGatewayOptions{}
	logger  = config.GetLogger("")
	sig     = make(chan os.Signal, 1)
)

func init() {
	lipgloss.SetColorProfile(termenv.ANSI256)

	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM, syscall.SIGABRT)
	// Set default options.
	options.JupyterGrpcPort = 8080
	options.ProvisionerPort = 8081
	options.ConnectionInfo.Transport = "tcp"

	gob.Register(metrics.ClusterStatistics{})
	gob.Register(metrics.ClusterEvent{})
	gob.Register(map[string]interface{}{})
	gob.Register(time.Duration(0))
	gob.Register(time.Time{})
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
func createAndStartDebugHttpServer() {
	// http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
	// 	log.Printf("Received HTTP debug connection to '/'")
	// 	w.WriteHeader(http.StatusOK)
	// 	w.Write([]byte(fmt.Sprintf("%d - Hello\n", http.StatusOK)))
	// })

	// http.HandleFunc("/test", func(w http.ResponseWriter, r *http.Request) {
	// 	log.Printf("Received HTTP debug connection to '/test'")
	// 	w.WriteHeader(http.StatusOK)
	// 	w.Write([]byte(fmt.Sprintf("%d - Test\n", http.StatusOK)))
	// })

	var address = fmt.Sprintf(":%d", options.DebugPort)
	log.Printf("Serving debug HTTP server: %s\n", address)

	if err := http.ListenAndServe(address, nil); err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}

// ValidateOptions ensures that the options/configuration is valid.
func ValidateOptions() {
	flags, err := config.ValidateOptions(&options)
	if errors.Is(err, config.ErrPrintUsage) {
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

	logger.Info("Starting cluster Gateway with the following options:\n%s\n", options.String())

	if options.PrettyPrintOptions {
		logger.Info("cluster Gateway ClusterGatewayOptions pretty-printed:\n%s\n", options.PrettyString(2))
	}

	if options.ClusterDaemonOptions.CommonOptions.DebugMode {
		go createAndStartDebugHttpServer()
	}

	daemon.CreateAndStartClusterGatewayComponents(&options, &done, finalize, sig)

	done.Wait()
}

func finalize(fix bool, identity string, distributedCluster *daemon.DistributedCluster) {
	if !fix {
		return
	}

	log.Printf("[WARNING] Finalize called with fix=%v and identity=\"%s\"\n", fix, identity)

	if err := recover(); err != nil {
		logger.Error("Called recover() and retrieved the following error: %v", err)

		if distributedCluster != nil {
			distributedCluster.HandlePanic(identity, err)
		}
	}

	logger.Error("Stack trace of CURRENT goroutine:")
	debug.PrintStack()

	logger.Error("Stack traces of ALL active goroutines:")
	err := pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
	if err != nil {
		logger.Error("Failed to output call stacks of all active goroutines: %v", err)
	}

	sig <- syscall.SIGINT
}
