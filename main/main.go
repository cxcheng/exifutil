package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/debug"
	"time"

	"github.com/cxcheng/exifutil"
)

func logElapsedTime(start time.Time, label string) {
	pc := make([]uintptr, 10) // at least 1 entry needed
	runtime.Callers(2, pc)
	f := runtime.FuncForPC(pc[0])
	//file, line := f.FileLine(pc[0])
	log.Printf("[Main] **** [%s]:[%s] elapsed time: %s, %d goroutines", label, f.Name(), time.Since(start), runtime.NumGoroutine())
}

func main() {
	defer func() {
		if state := recover(); state != nil {
			log.Printf("Exiting because of error...")
			debug.PrintStack()
		}
	}()

	var config *exifutil.Config
	var pipeline *exifutil.Pipeline
	var err error

	// Process input arguments
	var confPath string
	flag.StringVar(&confPath, "conf", "config.yml", "Path to configuration file")
	flag.Parse()
	if len(flag.Args()) < 1 {
		fmt.Fprintf(os.Stderr, "Usage: %s [<flags>] <pipeline-to-use> [<args>]\n", os.Args[0])
		os.Exit(1)
	}

	// Read config
	if config, err = exifutil.NewConfig(confPath); err != nil {
		log.Printf("[Main] %s", err)
		os.Exit(1)
	}

	// Setup log path
	var logF *os.File
	if config.LogPath != "" {
		var err error
		if logF, err = os.Create(config.LogPath); err == nil {
			log.SetOutput(logF)
		} else {
			log.SetOutput(os.Stderr)
		}
	}

	// Construct and initialize pipeline arguments
	if pipeline, err = exifutil.NewPipeline(config, flag.Args()[0]); err != nil {
		log.Fatalf("[Main] %s", err)
	}

	// Run pipeline after initializing command-line flags
	if err := pipeline.Run(); err != nil {
		log.Fatalf("[Main] %s", err)
	} else {
		os.Exit(0)
	}
}
