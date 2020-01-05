package exifutil

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sync"
	"time"
)

type MetadataInput struct {
	exitOnError bool
	fileExts    []*regexp.Regexp
	mimeTypes   []*regexp.Regexp
	out         PipelineChan

	readers []*MetadataReader
}

func (c *MetadataInput) Init(config *Config) error {
	var err error

	// Copy configs
	c.exitOnError = config.Input.ExitOnError
	c.fileExts = make([]*regexp.Regexp, 0, len(config.Input.FileExts))
	for _, pattern := range config.Input.FileExts {
		regex, err := regexp.Compile(pattern)
		if err != nil {
			return fmt.Errorf("[Input] Error parsing fileExt [%s]: %v", pattern, err)
		}
		c.fileExts = append(c.fileExts, regex)
	}
	c.mimeTypes = make([]*regexp.Regexp, 0, len(config.Input.MimeTypes))
	for _, pattern := range config.Input.MimeTypes {
		regex, err := regexp.Compile(pattern)
		if err != nil {
			return fmt.Errorf("[Input] Error parsing mimeType [%s]: %v", pattern, err)
		}
		c.mimeTypes = append(c.mimeTypes, regex)
	}

	// Set timezone if specified, otherwise, use local time zone
	var tz *time.Location
	if config.Input.Tz != "" {
		if tz, err = time.LoadLocation(config.Input.Tz); err != nil {
			log.Printf("[Input] Error loading TZ %s: %v", config.Input.Tz, err)
		}
	}
	if tz == nil {
		// Substitute with local timezone is none specified
		tz = time.Now().Local().Location()
	}

	// Setup tags to load map
	var tagsToLoadMap map[string]bool
	for _, tagToLoad := range config.Input.TagsToLoad {
		tagsToLoadMap[tagToLoad] = true
	}

	// Create reader pool based on Max CPU setting, but limited by number of CPU cores
	maxCPUs := config.Performance.MaxCPUs
	if maxCPUs == 0 {
		maxCPUs = 1
	}
	if maxCPUs > runtime.NumCPU() {
		maxCPUs = runtime.NumCPU()
	}
	log.Printf("[Input] Using %d readers", maxCPUs)

	for i := 0; i < maxCPUs; i++ {
		var r *MetadataReader
		if r, err = NewMetadataReader(config.Input.MetaConfig, tz, tagsToLoadMap); err != nil {
			break
		}
		c.readers = append(c.readers, r)
	}

	return err
}

func (c *MetadataInput) SetInput(in PipelineChan) {
	if in != nil {
		log.Fatalf("[Input] Input not allowed")
	}
}

func (c *MetadataInput) SetOutput(out PipelineChan) {
	c.out = out
}

func (c *MetadataInput) Run() error {
	// If no output, then exit
	if c.out == nil {
		return fmt.Errorf("[Input] No output defined")
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
					fileExt := filepath.Ext(path)
					if len(fileExt) > 1 {
						// fileExt is of the form ".*". We need to strip first .
						for _, ext := range c.fileExts {
							if matchedExt = ext.MatchString(fileExt[1:]); matchedExt {
								break
							}
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
			go func(i int, r *MetadataReader, paths []string) {
				start := time.Now()
				success := c.processInput(r, paths)
				if success {
					numSuccesses += len(paths)
				}
				log.Printf("[Input] Goroutine %d - %d files, elapsed time: %v, success: %v", i, len(paths), time.Since(start), success)
				wg.Done()
			}(i, c.readers[i], pathsToProcess)
		}
		wg.Wait()
		log.Printf("[Input] [%d] files, [%d] successes, [%d] errors", numFiles, numSuccesses, numFiles-numSuccesses)
	} else {
		log.Println("[Input] No files to process")
	}

	// Signal finish
	c.out <- nil
	return nil
}

func (c *MetadataInput) processInput(r *MetadataReader, paths []string) bool {
	var data []Metadata
	var err error
	data, err = r.ReadMetadata(paths)
	if err != nil {
		log.Printf("[Input] %v error: %s\n", paths, err)
		if c.exitOnError {
			// If error, notify to stop
			c.out <- nil
		}
		return false
	} else {
		// Filter by MIME type
		if len(c.mimeTypes) > 0 {
			filteredData := make([]Metadata, 0, len(data))
			for _, dataEntry := range data {
				matched := false
				for _, mimeType := range c.mimeTypes {
					if v, exists := dataEntry.V["MIMEType"]; exists {
						if dataMimeType, ok := v.(string); ok {
							if matched = mimeType.MatchString(dataMimeType); matched {
								break
							}
						}
					} else {
						// no MIMEType. Consider fail
						break
					}
				}
				if matched {
					filteredData = append(filteredData, dataEntry)
				}
			}
			// Replace with filtered list
			data = filteredData
		}

		c.out <- &PipelineObj{err: err, data: data}
		return true
	}
}
