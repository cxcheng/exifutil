package exifutil

import (
	"errors"
	"flag"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"
)

type MetadataInput struct {
	exitOnError   bool
	fileExts      []string
	out           PipelineChan
	tagsToLoadMap map[string]bool
	tz            *time.Location

	readers []*MetadataReader
}

func (c *MetadataInput) Init(config *Config) error {
	var err error

	// Copy configs
	c.exitOnError = config.ExitOnError
	c.fileExts = config.Input.FileExts
	for _, tagToLoad := range config.Input.TagsToLoad {
		c.tagsToLoadMap[tagToLoad] = true
	}

	// Set timezone if specified, otherwise, use local time zone
	if config.Input.Tz != "" {
		if c.tz, err = time.LoadLocation(config.Input.Tz); err != nil {
			log.Printf("Unable to load timezone %s", config.Input.Tz)
		}
	} else {
		// otherwise use local location
		c.tz = time.Now().Local().Location()
	}

	// Create reader pool based on Max CPU setting, but limited by number of CPU cores
	maxCPUs := config.Throttle.MaxCPUs
	if maxCPUs == 0 {
		maxCPUs = 1
	}
	if maxCPUs > runtime.NumCPU() {
		maxCPUs = runtime.NumCPU()
	}

	for i := 0; i < maxCPUs; i++ {
		var r *MetadataReader
		if r, err = MakeMetadataReader(config.Input.MetaConfig); err != nil {
			break
		}
		c.readers = append(c.readers, r)
	}

	return err
}

func (c *MetadataInput) SetInput(in PipelineChan) {
	if in != nil {
		log.Fatalf("Cannot set input to starting conent")
	}
}

func (c *MetadataInput) SetOutput(out PipelineChan) {
	c.out = out
}

func (c *MetadataInput) Run() error {
	// If no output, then exit
	if c.out == nil {
		return errors.New("No output defined")
	}

	// Walkthrough arguments to construct path lists
	pathLists := make([][]string, len(c.readers))
	currentList := 0
	numFiles := 0
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
					// Rotate the list to add them to
					pathLists[currentList] = append(pathLists[currentList], path)
					currentList++
					if currentList >= len(pathLists) {
						currentList = 0
					}
					numFiles++
				}
				return nil // ignore errors
			})
	}

	// Splits the load
	if numFiles > 0 {
		// Execute goroutines to process
		wg := sync.WaitGroup{}
		wg.Add(len(c.readers))
		numSuccesses := 0
		for i, pathsToProcess := range pathLists {
			go func(r *MetadataReader, paths []string) {
				start := time.Now()
				success := c.processInput(r, paths)
				if success {
					numSuccesses++
				}
				log.Printf("[%v] elapsed time: %s, success: %v", paths, time.Since(start), success)
				wg.Done()
			}(c.readers[i], pathsToProcess)
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

func (c *MetadataInput) processInput(r *MetadataReader, paths []string) bool {
	var data []Metadata
	var err error
	data, err = r.ReadMetadata(paths, c.tz, c.tagsToLoadMap)
	if err != nil {
		log.Printf("%v error: %s\n", paths, err)
		if c.exitOnError {
			// If error, notify to stop
			c.out <- nil
		}
		return false
	} else {
		c.out <- &PipelineObj{err: err, data: data}
		return true
	}
}
