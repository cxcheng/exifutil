package exifcommand

import (
	"encoding/csv"
	"errors"
	"flag"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type InfoCmd struct {
	conf     *Config
	showTags bool
}

func (cmd *InfoCmd) Init(conf *Config) error {
	cmd.conf = conf

	// Process command-line arguments
	flag.BoolVar(&cmd.showTaags, "tags", true, "Show all supported tags")
	flag.Parse()

	// Initialize configuration
	cmd.cols = strings.Split(cols, ",")
	cmd.trim = conf.Trim
	if outPath != "" {
		if f, err := os.OpenFile(outPath, os.O_RDWR|os.O_CREATE, 0666); err == nil {
			cmd.w = f
			defer f.Close()
		}
	}
	if cmd.w == nil {
		cmd.w = os.Stdout
	}

	// Set timezone if specified, otherwise, use local time zone
	if conf.Tz != "" {
		var err error
		if cmd.tz, err = time.LoadLocation(conf.Tz); err != nil {
			log.Printf("Unable to load timezone [%s]", conf.Tz)
			return err
		}
	} else {
		// otherwise use local location
		cmd.tz = time.Now().Local().Location()
	}

	// Check for output type and sort col
	if len(cmd.cols) > 1 && cmd.outType == "" {
		cmd.outType = "csv" // set type to csv
	}
	if cmd.outType == "csv" {
		if len(cmd.cols) == 0 {
			return errors.New("No columns specified for CSV")
		}
		cmd.csvW = csv.NewWriter(cmd.w)
	}
	if sortCol == "-" {
		cmd.sortColIdx = -1 // no sorting
	} else {
		cmd.sortColIdx = 0
		if sortCol != "" {
			// Check if ordering by specified tag
			for i, col := range cmd.cols {
				if sortCol == col {
					cmd.sortColIdx = i
					break
				}
			}
			if cmd.sortColIdx == -1 {
				log.Printf("Missing sort col [%s], ignored", sortCol)
			}
		}
	}

	return nil
}

func (cmd *InfoCmd) Run() {
	defer cmd.logElapsedTime(time.Now(), "Run")

	// Setup output and status channels
	cmd.out = make(chan outRecord)

	// Preliminary output of CSV headers
	if cmd.csvW != nil {
		cmd.csvW.Write(cmd.cols)
	}

	// Walkthrough arguments
	for _, arg := range flag.Args() {
		_ = filepath.Walk(arg,
			func(path string, f os.FileInfo, err error) error {
				// Skip directories
				if f.IsDir() {
					return nil
				}
				// Filter out file based on extension
				matchedExt := false
				if cmd.filterExt && len(cmd.conf.FileExts) > 0 {
					for _, ext := range cmd.conf.FileExts {
						if filepath.Ext(path) == "."+ext {
							matchedExt = true
						}
					}
				} else {
					matchedExt = true
				}
				// Proceed to create context to process file
				if !cmd.filterExt || matchedExt {
					if cmd.conf.Verbose {
						log.Printf("Processing [%s]", path)
					}
					// Execute goroutine to process
					cmd.numFiles++
					go cmd.generateOutput(path)
				}
				return nil // ignore errors
			})
	}

	// Wait for all the concurrent file processors to return
	numSuccesses, numErrors := 0, 0
	for numFiles := cmd.numFiles; numFiles > 0; numFiles-- {
		record := <-cmd.out
		if len(record.cols) == 0 {
			numErrors++
			println(">>>>", record.path)
			if cmd.conf.ExitOnFirstError {
				// Error received, exit if exit on first error
				log.Printf("Exiting on first error: [%s]", record.path)
				break
			}
		} else {
			numSuccesses++
			if cmd.sortColIdx >= 0 {
				// buffer for sorting later
				cmd.records = append(cmd.records, record)
			} else {
				cmd.writeOutput(&record)
			}
		}
	}
	if cmd.conf.Verbose {
		log.Printf("Processed [%d] files, [%d] successes, [%d] errors", cmd.numFiles, numSuccesses, numErrors)
	}

	// Sort and output any buffered record
	if cmd.sortColIdx >= 0 && cmd.sortColIdx < len(cmd.cols) {
		sort.Sort(cmd)
	}
	for _, record := range cmd.records {
		cmd.writeOutput(&record)
	}
}
