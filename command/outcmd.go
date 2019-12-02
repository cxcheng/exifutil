package exifcommand

import (
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
	cols  []string
	colVs []interface{}
}

type statusRecord struct {
	finished bool
	err      error
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
	status  chan statusRecord
	records []outRecord
}

func (cmd *OutCmd) Init(conf *Config) error {
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
			log.Fatalf("Unable to load timezone [%s]", conf.Tz)
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

func (cmd *OutCmd) sendString(s string) {
	cmd.sendRecord([]string{s}, []interface{}{s})
}

func (cmd *OutCmd) sendRecord(cols []string, colVs []interface{}) {
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
	cmd.out <- outRecord{cols: cols, colVs: colVs}
}

func (cmd *OutCmd) Run() {
	// Setup output and status channels
	cmd.out = make(chan outRecord)
	cmd.status = make(chan statusRecord)

	// Preliminary output of CSV headers
	if cmd.csvW != nil {
		cmd.csvW.Write(cmd.cols)
	}

	// Walkthrough arguments
	numFiles := 0
	for _, arg := range flag.Args() {
		err := filepath.Walk(arg,
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
					numFiles += 1
					go cmd.generateOutput(path)
				}
				return nil // ignore errors
			})
		if err != nil {

		}
	}

	// Wait for all the concurrent file processors to return
	for numFiles > 0 {
		record := <-cmd.out
		numFiles -= 1 // one file returned
		if len(record.cols) == 0 {
			// signal that all files has been sent for processing
		}
		if cmd.sortColIdx >= 0 {
			// buffer
			cmd.records = append(cmd.records, record)
		} else {
			cmd.writeOutput(&record)
		}
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
	exifData, err := exifutil.ReadExifData(path, cmd.tz, cmd.conf.Trim, cmd.conf.Tags)
	if err != nil {
		log.Fatalf("[%s]: [%s]\n", path, err)
	} else {
		// Apply optional filter
		if cmd.filter == "" || exifData.Filter(cmd.filter) {
			switch cmd.outType {
			case "json":
				cmd.sendString(exifData.Json())
			case "keys":
				for _, key := range exifData.Keys() {
					cmd.sendString(key)
				}
			default:
				// if cols specified, evaluate; otherwise, print every field
				if len(cmd.cols) > 0 {
					outCols := make([]string, len(cmd.cols))
					outColVs := make([]interface{}, len(cmd.cols))
					for i, col := range cmd.cols {
						outCols[i], outColVs[i] = exifData.Expr(col)
					}
					cmd.sendRecord(outCols, outColVs)
				} else {
					cmd.sendString(exifData.String())
				}
			}
		}
	}
}

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
