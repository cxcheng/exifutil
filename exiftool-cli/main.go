// This tool dumps EXIF information from images.
//
// Example command-line:
//
//   exif-read-tool -filepath <file-path>
//

package main

import (
	"flag"
	"log"
	"os"
	"runtime/debug"

	"github.com/cxcheng/exifutil/exiftool"
)

func main() {
	defer func() {
		if state := recover(); state != nil {
			log.Printf("Exiting because of error...")
			debug.PrintStack()
		}
	}()

	// Read configuration if specified or default
	var confPath string
	flag.StringVar(&confPath, "conf", "exif.yml", "Path of optional config YAML")

	// Initialize and run pipeline components
	var pipeline *exiftool.Pipeline
	var err error
	if pipeline, err = exiftool.MakePipeline(confPath); err != nil {
		log.Printf("Error creating pipeline: %s", err)
		os.Exit(2)
	}
	pipeline.Run()
	os.Exit(0)
}
