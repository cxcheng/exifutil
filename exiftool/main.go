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

	exifcommand "github.com/cxcheng/exifutil/command"
)

func main() {
	defer func() {
		if state := recover(); state != nil {
			log.Printf("Exiting because of error...")
		}
	}()

	conf := exifcommand.MakeConfig()
	if !conf.Verbose {
		verbose := false
		flag.BoolVar(&verbose, "verbose", false, "Verbose output, overrides config")
	}

	cmd := exifcommand.OutCmd{}
	if err := cmd.Init(conf); err != nil {
		log.Printf("Error initiializing: %s", err)
		os.Exit(1)
	}

	cmd.Run()

	// Finally, done
	os.Exit(0)
}
