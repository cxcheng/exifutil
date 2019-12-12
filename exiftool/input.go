package exiftool

import (
	"errors"
	"flag"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/cxcheng/exifutil"
)

type ExifInput struct {
	exitOnError bool
	fileExts    []string
	out         PipelineChan
	trim        bool
	tagsToLoad  []string
	tz          *time.Location
}

func (c *ExifInput) Init(config *Config) error {
	var err error

	// Copy configs
	c.exitOnError = config.ExitOnError
	c.fileExts = config.Input.FileExts
	c.trim = config.Input.Trim
	c.tagsToLoad = config.Input.TagsToLoad

	// Set timezone if specified, otherwise, use local time zone
	if config.Input.Tz != "" {
		if c.tz, err = time.LoadLocation(config.Input.Tz); err != nil {
			log.Printf("Unable to load timezone %s", config.Input.Tz)
		}
	} else {
		// otherwise use local location
		c.tz = time.Now().Local().Location()
	}

	return err
}

func (c *ExifInput) SetInput(in PipelineChan) {
	if in != nil {
		log.Fatalf("Cannot set input to starting conent")
	}
}

func (c *ExifInput) SetOutput(out PipelineChan) {
	c.out = out
}

func (c *ExifInput) Run() error {
	// If no output, then exit
	if c.out == nil {
		return errors.New("No output defined")
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
				if len(c.fileExts) > 0 {
					for _, ext := range c.fileExts {
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
				success := c.processInput(path)
				if success {
					numSuccesses++
				}
				log.Printf("[%s] elapsed time: %s, success: %v", path, time.Since(start), success)
				wg.Done()
			}(fileToProcess)
		}
		wg.Wait()
		log.Printf("Finished input: [%d] files, [%d] successes, [%d] errors", numFiles, numSuccesses, numFiles-numSuccesses)
	} else {
		log.Println("No files to process")
	}

	// Signal finish
	c.out <- nil
	return nil
}

func (c *ExifInput) processInput(path string) bool {
	exifData, err := exifutil.ReadExifData(path, c.tz, c.trim, c.tagsToLoad)
	if err != nil {
		log.Printf("[%s] error: %s\n", path, err)
		if c.exitOnError {
			// If error, notify to stop
			c.out <- nil
		}
		return false
	} else {
		c.out <- exifData
		return true
	}
}
