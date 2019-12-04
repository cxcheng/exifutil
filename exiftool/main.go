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
	"runtime"
	"runtime/debug"

	exifcommand "github.com/cxcheng/exifutil/command"
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
	conf := exifcommand.MakeConfig(confPath)

	// Setup verbose param
	if !conf.Verbose {
		flag.BoolVar(&conf.Verbose, "verbose", false, "Verbose output, overrides config")
	}
	if conf.Verbose {
		log.Printf("Number of CPUs: %d", runtime.NumCPU())
	}

	// Run command
	cmd := exifcommand.OutCmd{}
	if err := cmd.Init(conf); err != nil {
		log.Printf("Error initiializing: %s", err)
		os.Exit(1)
	}
	cmd.Run()

	// Exit
	os.Exit(0)
}
