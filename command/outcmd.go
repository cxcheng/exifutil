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

func (cmd *OutCmd) Run() {
	// Walkthrough arguments
	var err error
	ctx := outCtx{cmd: cmd}

	// Preliminary output of CSV headers
	if ctx.cmd.csvW != nil {
		ctx.cmd.csvW.Write(ctx.cmd.cols)
	}

	for _, arg := range flag.Args() {
		err = filepath.Walk(arg,
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
					err = ctx.process(path, f, err)
					if err != nil && cmd.conf.ExitOnFirstError {
						// return on first error if flag set
						return err
					}
				}
				return nil // ignore errors
			})
	}
	// If no error, finish the processing by sorting and emptying buffered input
	if err == nil {
		ctx.finish()

	}
}

type outCtx struct {
	cmd     *OutCmd
	records []outRecord
}

type outRecord struct {
	cols  []string
	colVs []interface{}
}

func (ctx *outCtx) process(path string, f os.FileInfo, err error) error {
	exifData, err := exifutil.ReadExifData(path, ctx.cmd.tz, ctx.cmd.conf.Trim, ctx.cmd.conf.Tags)
	if err != nil {
		log.Fatalf("[%s]: [%s]\n", path, err)
	} else {
		// Apply optional filter
		if ctx.cmd.filter == "" || exifData.Filter(ctx.cmd.filter) {
			switch ctx.cmd.outType {
			case "json":
				ctx.out(exifData.Json())
			case "keys":
				for _, key := range exifData.Keys() {
					ctx.out(key)
				}
			default:
				// if cols specified, evaluate; otherwise, print every field
				if len(ctx.cmd.cols) > 0 {
					outCols := make([]string, len(ctx.cmd.cols))
					outColVs := make([]interface{}, len(ctx.cmd.cols))
					for i, col := range ctx.cmd.cols {
						outCols[i], outColVs[i] = exifData.Expr(col)
					}
					ctx.outRecord(outRecord{
						cols:  outCols,
						colVs: outColVs,
					})
				} else {
					ctx.out(exifData.String())
				}
			}
		}
	}
	return err
}

func (ctx *outCtx) flush() {
	if ctx.cmd.csvW != nil {
		ctx.cmd.csvW.Flush()
	}
}

func (ctx *outCtx) out(s string) {
	ctx.outRecord(outRecord{
		cols:  []string{s},
		colVs: []interface{}{s},
	})
}

func (ctx *outCtx) outRecord(r outRecord) {
	if ctx.cmd.sortColIdx >= 0 {
		// buffer
		ctx.records = append(ctx.records, r)
	} else if ctx.cmd.csvW != nil {
		if ctx.cmd.value {
			cols := make([]string, len(r.cols))
			for i, colV := range r.colVs {
				cols[i] = fmt.Sprintf("%v", colV)
			}
			ctx.cmd.csvW.Write(cols)
		} else {
			ctx.cmd.csvW.Write(r.cols)
		}
	} else {
		// only 1 column written if not in CSV mode
		if ctx.cmd.value {
			fmt.Fprintln(ctx.cmd.w, r.colVs[0])
		} else {
			fmt.Fprintln(ctx.cmd.w, r.cols[0])
		}
	}
}

func (ctx *outCtx) finish() {
	// Sort
	if ctx.cmd.sortColIdx >= 0 && ctx.cmd.sortColIdx < len(ctx.cmd.cols) {
		sort.Sort(ctx)
		ctx.cmd.sortColIdx = -1 // mark as done, or this becomes recursive
	}

	// Output
	for _, r := range ctx.records {
		ctx.outRecord(r)
	}
	ctx.flush()
}

func (ctx *outCtx) Len() int {
	return len(ctx.records)
}

func (ctx *outCtx) Less(i, j int) bool {
	less := false
	ok := false
	vi := ctx.records[i].colVs[ctx.cmd.sortColIdx]
	switch vi.(type) {
	case string:
		var vj string
		if vj, ok = ctx.records[j].colVs[ctx.cmd.sortColIdx].(string); ok {
			less = vi.(string) < vj
		}
	case float64:
		var vj float64
		if vj, ok = ctx.records[j].colVs[ctx.cmd.sortColIdx].(float64); ok {
			less = vi.(float64) < vj
		}
	case int64:
		var vj int64
		if vj, ok = ctx.records[j].colVs[ctx.cmd.sortColIdx].(int64); ok {
			less = vi.(int64) < vj
		}
	}
	if ctx.cmd.sortReverse {
		return ok && !less
	} else {
		return ok && less
	}
}

func (ctx *outCtx) Swap(i, j int) {
	ctx.records[i], ctx.records[j] = ctx.records[j], ctx.records[i]
}
