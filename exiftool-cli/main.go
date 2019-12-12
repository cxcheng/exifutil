// This tool dumps EXIF information from images.
//
// Example command-line:
//
//   exif-read-tool -filepath <file-path>
//

package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/debug"
	"strings"
	"time"

	"github.com/cxcheng/exifutil/exiftool"
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

	var config *exiftool.Config
	var pipeline *exiftool.Pipeline
	var err error

	// Read and process descriptor file
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s [<flags>] <descriptor.yml> [<input-files>]\n", os.Args[0])
		os.Exit(1)
	}

	// Find first argument that's not a flag and use it as path to config file
	firstNonFlagIdx := 1
	for firstNonFlagIdx < len(os.Args) {
		if !strings.HasPrefix(os.Args[firstNonFlagIdx], "-") {
			break
		}
		firstNonFlagIdx++
	}
	if firstNonFlagIdx >= len(os.Args) {
		fmt.Fprintf(os.Stderr, "Usage: %1 [<flags>] <pipeline.yml> <inputs>\n", os.Args[0])
		os.Exit(1)
	}

	config, err = exiftool.MakeConfig(os.Args[firstNonFlagIdx])
	if err != nil {
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
	if pipeline, err = exiftool.MakePipeline(config); err != nil {
		log.Printf("%s", err)
		os.Exit(2)
	}

	// Run pipeline after initializing command-line flags
	flag.Parse()
	if err := pipeline.Run(); err != nil {
		log.Printf("Pipeline error: %s", err)
		os.Exit(2)
	} else {
		os.Exit(0)
	}
}
