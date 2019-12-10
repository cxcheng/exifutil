package exiftool

import (
	"flag"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/cxcheng/exifutil"
)

type ExifInput struct {
	conf *Config
	out  *PipelineChan
	tz   *time.Location
}

func MakeInput(conf *Config) (*PipelineComponent, error) {
	var ctx ExifInput
	var err error

	ctx = ExifInput{conf: conf, out: new(PipelineChan)}

	// Process command-line arguments
	var filterExt bool
	flag.BoolVar(&filterExt, "filterext", true, "Filter files by extension")
	if !filterExt {
		ctx.conf.Input.FileExts = make([]string, 0) // no file extensions to filter
	}

	// Set timezone if specified, otherwise, use local time zone
	if ctx.conf.Input.Tz != "" {
		var err error
		if ctx.tz, err = time.LoadLocation(ctx.conf.Input.Tz); err != nil {
			log.Printf("Unable to load timezone [%s]", ctx.conf.Input.Tz)
		}
	} else {
		// otherwise use local location
		ctx.tz = time.Now().Local().Location()
	}

	return &ctx, err
}

func (ctx *ExifInput) SetInput(in *PipelineChan) {
	if in != nil {
		log.Fatalf("Cannot set input to starting component")
	}
}

func (ctx *ExifInput) GetOutput() *PipelineChan {
	return ctx.out
}

func (ctx *ExifInput) Run(callOnExit func(time.Time, string)) {
	defer callOnExit(time.Now(), "Read Inputs")

	// Walkthrough arguments
	numFiles := 0
	for _, arg := range flag.Args() {
		_ = filepath.Walk(arg,
			func(path string, f os.FileInfo, err error) error {
				// Skip directories
				if f.IsDir() {
					return nil
				}
				// Filter out file based on extension
				matchedExt := false
				if len(ctx.conf.Input.FileExts) > 0 {
					for _, ext := range ctx.conf.Input.FileExts {
						if filepath.Ext(path) == "."+ext {
							matchedExt = true
						}
					}
				} else {
					matchedExt = true
				}
				// Proceed to create context to process file
				if matchedExt {
					if ctx.conf.Verbose {
						log.Printf("Processing [%s]", path)
					}
					// Execute goroutine to process
					numFiles++
					go ctx.processInput(callOnExit, path)
				}
				return nil // ignore errors
			})
	}

	// Wait for all the concurrent file processors to return
	numSuccesses, numErrors := 0, 0
	/*
		for n := numFiles; n > 0; n-- {
			record := <-ctx.out
			if len(record.cols) == 0 {
				numErrors++
				if ctx.Conf.ExitOnError {
					// Error received, exit if exit on first error
					log.Printf("Exiting on first error: [%s]", record.path)
					break
				}
			} else {
				numSuccesses++
				if ctx.sortColIdx >= 0 {
					// buffer for sorting later
					ctx.records = append(ctx.records, record)
				} else {
					ctx.writeOutput(&record)
				}
			}
		}
	*/
	if ctx.conf.Verbose {
		log.Printf("Processed [%d] files, [%d] successes, [%d] errors", numFiles, numSuccesses, numErrors)
	}

	// Signal finish
	*ctx.out <- nil
}

func (ctx *ExifInput) processInput(callOnExit func(time.Time, string), path string) {
	defer callOnExit(time.Now(), "Process "+path)

	exifData, err := exifutil.ReadExifData(path, ctx.tz, ctx.conf.Input.Trim, ctx.conf.Input.TagsToLoad)
	if err != nil {
		log.Printf("Error processing [%s]: %s\n", path, err)
		if ctx.conf.ExitOnError {
			// Output nil to stop
			*ctx.out <- nil

		}
	} else {
		*ctx.out <- exifData
	}
}
