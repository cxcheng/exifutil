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
	log.Printf("**** [%s]:[%s] elapsed time: %s, %d goroutines", label, f.Name(), time.Since(start), runtime.NumGoroutine())
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
	pipelineArgs := exifutil.AddArgs()
	flag.Parse()
	if len(flag.Args()) < 1 {
		fmt.Fprintf(os.Stderr, "Usage: %s [<config>] <pipeline> [<args>]\n", os.Args[0])
		os.Exit(1)
	}

	// Read config
	if config, err = exifutil.MakeConfig(pipelineArgs.ConfPath); err != nil {
		log.Printf("%s", err)
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
	if pipeline, err = exifutil.MakePipeline(config, flag.Args()[0]); err != nil {
		log.Printf("%s", err)
		os.Exit(2)
	}

	// Run pipeline after initializing command-line flags
	if err := pipeline.Run(); err != nil {
		log.Printf("Pipeline error: %s", err)
		os.Exit(2)
	} else {
		os.Exit(0)
	}
}
