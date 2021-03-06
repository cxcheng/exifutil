// This tool dumps EXIF information from images.
//
// Example command-line:
//
//   exif-read-tool -filepath <file-path>
//

package main

import (
	"log"
	"os"

	exifcommand "github.com/cxcheng/exifutil/command"
)

func main() {
	defer func() {
		if state := recover(); state != nil {
			log.Fatal("Exiting because of error...")
		}
	}()

	conf := exifcommand.MakeConfig()
	cmd := exifcommand.OutCmd{}
	if err := cmd.Init(conf); err != nil {
		log.Printf("Error initiializing: %s", err)
		os.Exit(1)
	}

	cmd.Run()

	// Finally, done
	os.Exit(0)
}
