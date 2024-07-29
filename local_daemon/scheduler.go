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

	"github.com/charmbracelet/lipgloss"
	"github.com/mason-leap-lab/go-utils/config"
	"github.com/muesli/termenv"

	"github.com/zhangjyr/distributed-notebook/local_daemon/daemon"
	"github.com/zhangjyr/distributed-notebook/local_daemon/domain"
)

const (
	ServiceName = "scheduler"
)

var (
	options domain.LocalDaemonOptions = domain.LocalDaemonOptions{}
	logger                            = config.GetLogger("")
	sig                               = make(chan os.Signal, 1)
)

func init() {
	lipgloss.SetColorProfile(termenv.ANSI256)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM, syscall.SIGABRT)
	// Set default options.
	options.Port = 8080
	options.ConnectionInfo.Transport = "tcp"
}

func ValidateOptions() {
	flags, err := config.ValidateOptions(&options)
	if err == config.ErrPrintUsage {
		flags.PrintDefaults()
		os.Exit(0)
	} else if err != nil {
		log.Fatal(err)
	}
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
	log.Printf("Serving debug HTTP server on port %d.\n", options.DebugPort)

	// http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
	// 	w.WriteHeader(http.StatusOK)
	// 	w.Write([]byte(fmt.Sprintf("%d - Hello\n", http.StatusOK)))
	// })

	// http.HandleFunc("/test", func(w http.ResponseWriter, r *http.Request) {
	// 	w.WriteHeader(http.StatusOK)
	// 	w.Write([]byte(fmt.Sprintf("%d - Test\n", http.StatusOK)))
	// })

	if err := http.ListenAndServe(fmt.Sprintf("localhost:%d", options.DebugPort), nil); err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}

func main() {
	defer finalize(false)

	var done sync.WaitGroup

	ValidateOptions()

	logger.Info("Starting local daemon (scheduler daemon) with options: %v", options)

	if options.DebugMode {
		go createAndStartDebugHttpServer()
	}

	daemon.CreateAndStartLocalDaemonComponents(&options, &done, finalize, sig)

	done.Wait()
}

func finalize(fix bool) {
	if !fix {
		return
	}

	if err := recover(); err != nil {
		logger.Error("%v", err)
	}

	log.Println("Finalize called. Will be terminating.")

	sig <- syscall.SIGINT
}
