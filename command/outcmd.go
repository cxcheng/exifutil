package exifcommand

import (
	"C"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	exifutil "github.com/cxcheng/exifutil"
)

type outRecord struct {
	path  string
	cols  []string
	colVs []interface{}
}

type OutCmd struct {
	conf        *Config
	w           *os.File
	csvW        *csv.Writer
	filter      string
	cols        []string
	outType     string
	sortColIdx  int
	sortReverse bool
	trim        bool
	tz          *time.Location
	value       bool

	out     chan outRecord
	records []outRecord
}

func logElapsedTime(start time.Time, label string) {
	elapsed := time.Since(start)
	log.Printf("**** [%s] elapsed time: %s", label, elapsed)
}

func (cmd *OutCmd) Init(conf *Config) error {
	defer logElapsedTime(time.Now(), "Init")

	cmd.conf = conf

	// Process command-line arguments
	var cols, outPath, sortCol string
	flag.StringVar(&cols, "cols", "Sys/Name,Make,Model,DateTimeOriginal", "Columns to output")
	flag.StringVar(&cmd.filter, "filter", "", "Expression to filter")
	flag.StringVar(&sortCol, "sort", "-", "Sort column")
	flag.BoolVar(&cmd.sortReverse, "reverse", false, "Reverse sort order")
	flag.StringVar(&outPath, "out", "", "Output path")
	flag.StringVar(&cmd.outType, "type", "", "Output type: csv, json, keys")
	flag.BoolVar(&cmd.value, "value", false, "Output value instead of original text")
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

func (cmd *OutCmd) sendString(path string, s string) {
	cmd.sendRecord(path, []string{s}, []interface{}{s})
}

func (cmd *OutCmd) sendRecord(path string, cols []string, colVs []interface{}) {
	if len(colVs) == 0 {
		colVs = make([]interface{}, len(cols))
		for i, col := range cols {
			colVs[i] = col
		}
	} else if len(cols) != len(colVs) {
		log.Printf("Skipping improperly formatted record")
	} else if cmd.value {
		for i, colV := range colVs {
			// replace original value with the stringified numeric version
			cols[i] = fmt.Sprintf("%v", colV)
		}
	}
	cmd.out <- outRecord{path: path, cols: cols, colVs: colVs}
}

func (cmd *OutCmd) Run() {
	defer logElapsedTime(time.Now(), "Run")

	// Setup output and status channels
	cmd.out = make(chan outRecord)

	// Preliminary output of CSV headers
	if cmd.csvW != nil {
		cmd.csvW.Write(cmd.cols)
	}

	// Walkthrough arguments
	numFiles := 0
	for _, arg := range flag.Args() {
		_ = filepath.Walk(arg,
			func(path string, f os.FileInfo, err error) error {
				// Filter out file based on extension
				matchedExt := false
				if len(cmd.conf.FileExts) > 0 {
					for _, ext := range cmd.conf.FileExts {
						if filepath.Ext(path) == "."+ext {
							matchedExt = true
						}
					}
				} else {
					matchedExt = true
				}
				// Proceed to create context to process file
				if matchedExt {
					if cmd.conf.Verbose {
						log.Printf("Processing [%s]", path)
					}
					// Execute goroutine to process
					numFiles++
					go cmd.generateOutput(path)
				}
				return nil // ignore errors
			})
	}

	// Wait for all the concurrent file processors to return
	numSuccesses, numErrors := 0, 0
	for numToWait := numFiles; numToWait > 0; numToWait-- {
		record := <-cmd.out
		if len(record.cols) == 0 {
			numErrors++
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
		log.Printf("Processed [%d] files, [%d] successes, [%d] errors", numFiles, numSuccesses, numErrors)
	}

	// Sort and output any buffered record
	if cmd.sortColIdx >= 0 && cmd.sortColIdx < len(cmd.cols) {
		sort.Sort(cmd)
	}
	for _, record := range cmd.records {
		cmd.writeOutput(&record)
	}
}

func (cmd *OutCmd) writeOutput(record *outRecord) {
	if cmd.csvW != nil {
		cmd.csvW.Write(record.cols)
		cmd.csvW.Flush()
	} else {
		// only 1 column written if not in CSV mode
		fmt.Fprintln(cmd.w, record.cols[0])
	}
}

func (cmd *OutCmd) generateOutput(path string) {
	defer logElapsedTime(time.Now(), path)

	exifData, err := exifutil.ReadExifData(path, cmd.tz, cmd.conf.Trim, cmd.conf.Tags)
	if err != nil {
		log.Printf("Error processing [%s]: [%s]\n", path, err)
		// Report error
		cmd.sendRecord(path, []string{}, []interface{}{})
	} else {
		// Apply optional filter
		if cmd.filter == "" || exifData.Filter(cmd.filter) {
			switch cmd.outType {
			case "json":
				cmd.sendString(path, exifData.Json())
			case "keys":
				for _, key := range exifData.Keys() {
					cmd.sendString(path, key)
				}
			default:
				// if cols specified, evaluate; otherwise, print every field
				if len(cmd.cols) > 0 {
					outCols := make([]string, len(cmd.cols))
					outColVs := make([]interface{}, len(cmd.cols))
					for i, col := range cmd.cols {
						outCols[i], outColVs[i] = exifData.Expr(col)
					}
					cmd.sendRecord(path, outCols, outColVs)
				} else {
					cmd.sendString(path, exifData.String())
				}
			}
		}
	}
}

/* Len(), Less(), Swap() implemented for sorting */

func (cmd *OutCmd) Len() int {
	return len(cmd.records)
}

func (cmd *OutCmd) Less(i, j int) bool {
	less := false
	ok := false
	vi := cmd.records[i].colVs[cmd.sortColIdx]
	switch vi.(type) {
	case string:
		var vj string
		if vj, ok = cmd.records[j].colVs[cmd.sortColIdx].(string); ok {
			less = vi.(string) < vj
		}
	case float64:
		var vj float64
		if vj, ok = cmd.records[j].colVs[cmd.sortColIdx].(float64); ok {
			less = vi.(float64) < vj
		}
	case int64:
		var vj int64
		if vj, ok = cmd.records[j].colVs[cmd.sortColIdx].(int64); ok {
			less = vi.(int64) < vj
		}
	}
	if cmd.sortReverse {
		return ok && !less
	} else {
		return ok && less
	}
}

func (cmd *OutCmd) Swap(i, j int) {
	cmd.records[i], cmd.records[j] = cmd.records[j], cmd.records[i]
}
