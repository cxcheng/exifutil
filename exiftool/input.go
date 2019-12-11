package exiftool

import (
	"flag"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/cxcheng/exifutil"
)

type ExifInput struct {
	config    *Config
	filterExt bool
	out       PipelineChan
	tz        *time.Location
}

func (ctx *ExifInput) AddArgs() {
	// Add command-line args
	flag.BoolVar(&ctx.filterExt, "filterext", true, "Filter files by extension")
}

func (ctx *ExifInput) Init(config *Config) error {
	var err error

	ctx.config = config
	ctx.out = make(PipelineChan)

	// Set timezone if specified, otherwise, use local time zone
	if config.Input.Tz != "" {
		if ctx.tz, err = time.LoadLocation(config.Input.Tz); err != nil {
			log.Printf("Unable to load timezone %s", config.Input.Tz)
		}
	} else {
		// otherwise use local location
		ctx.tz = time.Now().Local().Location()
	}

	return err
}

func (ctx *ExifInput) SetInput(in PipelineChan) {
	if in != nil {
		log.Fatalf("Cannot set input to starting component")
	}
}

func (ctx *ExifInput) GetOutput() PipelineChan {
	return ctx.out
}

func (ctx *ExifInput) Run(callOnExit func(time.Time, string)) {
	defer callOnExit(time.Now(), "Read Inputs")

	// Setup filtering configuration
	if ctx.filterExt {
		ctx.config.Input.FileExts = make([]string, 0) // no file extensions to filter
	}

	// Walkthrough arguments
	filesToProcess := []string{}
	for _, arg := range flag.Args()[1:] {
		_ = filepath.Walk(arg,
			func(path string, f os.FileInfo, err error) error {

				// Skip directories or unknown
				if f == nil || f.IsDir() {
					return nil
				}
				// Filter out file based on extension
				matchedExt := false
				if len(ctx.config.Input.FileExts) > 0 {
					for _, ext := range ctx.config.Input.FileExts {
						if filepath.Ext(path) == "."+ext {
							matchedExt = true
						}
					}
				} else {
					matchedExt = true
				}
				if matchedExt {
					// If not filtered out, add to list to process
					filesToProcess = append(filesToProcess, path)
				}
				return nil // ignore errors
			})
	}

	numFiles := len(filesToProcess)
	if numFiles > 0 {
		// Execute goroutines to process
		wg := sync.WaitGroup{}
		wg.Add(numFiles)
		numSuccesses := 0
		for _, fileToProcess := range filesToProcess {
			go func(path string) {
				start := time.Now()
				success := ctx.processInput(path)
				if success {
					numSuccesses++
				}
				log.Printf("[%s] elapsed time: %s, success: %v", path, time.Since(start), success)
				wg.Done()
			}(fileToProcess)
		}
		wg.Wait()
		if ctx.config.Verbose {
			log.Printf("Finished input: [%d] files, [%d] successes, [%d] errors", numFiles, numSuccesses, numFiles-numSuccesses)
		}
	} else {
		log.Println("No files to process")
	}

	// Signal finish
	ctx.out <- nil
}

func (ctx *ExifInput) processInput(path string) bool {
	exifData, err := exifutil.ReadExifData(path, ctx.tz, ctx.config.Input.Trim, ctx.config.Input.TagsToLoad)
	if err != nil {
		log.Printf("[%s] error: %s\n", path, err)
		if ctx.config.ExitOnError {
			// If error, notify to stop
			ctx.out <- nil
		}
		return false
	} else {
		ctx.out <- exifData
		return true
	}
}
