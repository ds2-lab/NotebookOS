package main

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime/debug"
	"runtime/pprof"
	"sync"
	"syscall"

	"github.com/Scusemua/go-utils/config"
	"github.com/charmbracelet/lipgloss"
	"github.com/muesli/termenv"

	"github.com/scusemua/distributed-notebook/local_daemon/daemon"
	"github.com/scusemua/distributed-notebook/local_daemon/domain"
)

const (
	ServiceName = "scheduler"
)

var (
	options = domain.LocalDaemonOptions{}
	logger  = config.GetLogger("")
	sig     = make(chan os.Signal, 1)
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
	if errors.Is(err, config.ErrPrintUsage) {
		flags.PrintDefaults()
		os.Exit(0)
	} else if err != nil {
		log.Fatal(err)
	}

	options.SchedulerOptions.ValidateClusterSchedulerOptions()
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
	defer func() {
		logger.Warn("Main goroutine for Local Daemon process is calling finalize(false) now...")
		finalize(false)
	}()

	var done sync.WaitGroup

	ValidateOptions()

	logger.Info("Starting local daemon (scheduler daemon) with options: %v", options)

	if options.SchedulerDaemonOptions.CommonOptions.DebugMode {
		go createAndStartDebugHttpServer()
	}

	daemon.CreateAndStartLocalDaemonComponents(&options, &done, finalize, sig)

	done.Wait()
}

func finalize(fix bool) {
	if !fix {
		logger.Warn("Finalize called, but `fix` is false, so we'll just return (rather than terminate the process).")
		return
	}

	if err := recover(); err != nil {
		logger.Error("Finalize called, then recover() called. Got back the following error: %v", err)
	}

	logger.Error("Finalize called. Will be terminating.")
	logger.Error("Stack trace of CURRENT goroutine:")
	debug.PrintStack()

	logger.Error("Stack traces of ALL active goroutines:")
	err := pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
	if err != nil {
		logger.Error("Failed to output call stacks of all active goroutines: %v", err)
	}

	sig <- syscall.SIGINT
}
