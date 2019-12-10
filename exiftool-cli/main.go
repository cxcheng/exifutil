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
	"time"

	"github.com/cxcheng/exifutil/exiftool"
	"gopkg.in/yaml.v2"
)

func logElapsedTime(start time.Time, label string) {
	pc := make([]uintptr, 10) // at least 1 entry needed
	runtime.Callers(2, pc)
	f := runtime.FuncForPC(pc[0])
	//file, line := f.FileLine(pc[0])
	log.Printf("**** [%s:%s] elapsed time: %s, %d goroutines", label, f.Name(), time.Since(start), runtime.NumGoroutine())
}

func main() {
	defer func() {
		if state := recover(); state != nil {
			log.Printf("Exiting because of error...")
			debug.PrintStack()
		}
	}()

	// Read and process descriptor file
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s [<flags>] <descriptor.yml> [<input-files>]\n", os.Args[0])
		os.Exit(1)
	}
	config := new(exiftool.Config)
	if f, err := os.Open(os.Args[1]); err == nil {
		decoder := yaml.NewDecoder(f)
		if err = decoder.Decode(config); err != nil {
			log.Panicf("Error reading pipeline config [%s], exiting...", p.confPath)
		}
		defer f.Close() // close immediately after exiting this
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
	if pipeline, err := MakePipeline(config); err != nil {
		log.Printf("%s", err)
		os.Exit(2)
	}

	// Run pipeline after initializing command-line flags
	flag.Parse()
	pipeline.Run(logElapsedTime)
	os.Exit(0)
}
