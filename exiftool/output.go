package exiftool

import (
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"
)

type ExifOutput struct {
	config *Config
	in     PipelineChan

	colsArg   string
	outType   string
	useValues bool
}

func (ctx *ExifOutput) AddArgs() {
	// Add command-line args
	flag.StringVar(&ctx.colsArg, "cols", "", "Columns to output")
	flag.StringVar(&ctx.outType, "type", "", "Output type: csv, json, keys")
	flag.BoolVar(&ctx.useValues, "values", false, "Output value instead of original text")

}

func (ctx *ExifOutput) Init(config *Config) error {
	ctx.config = config

	// Setup output type
	if ctx.outType == "" {
		ctx.outType = "csv"
	}

	return nil
}

func (ctx *ExifOutput) SetInput(in PipelineChan) {
	ctx.in = in
}

func (ctx *ExifOutput) GetOutput() PipelineChan {
	return nil
}

func (ctx *ExifOutput) Run(callOnExit func(time.Time, string)) {
	defer callOnExit(time.Now(), "Output")

	// Setup default output columns
	var cols []string
	if ctx.colsArg == "" {
		if len(ctx.config.Input.TagsToLoad) > 0 {
			cols = ctx.config.Input.TagsToLoad
		} else {
			cols = []string{"Sys/Name", "Sys/Key", "Make", "Model", "DateTimeOriginal"}
		}
	} else {
		cols = strings.Split(ctx.colsArg, ",")
	}

	// Setup output file
	var w *os.File
	var err error
	if w, err = os.Create(ctx.config.Output.Path); err != nil {
		// Substitute with Stdout
		w = os.Stdout
	}
	defer func() { w.Close() }() // close file on exit

	// Setup for CSV if specified, or multi-cols
	var csvW *csv.Writer
	if ctx.outType == "csv" {
		csvW = csv.NewWriter(w)

		// Output headers
		csvW.Write(cols)
	}

	// Process incoming records
	for {
		exifData := <-ctx.in
		if exifData == nil {
			// No more inputs, exit
			break
		}
		switch ctx.outType {
		case "csv":
			outCols := make([]string, len(cols))
			if ctx.useValues {
				for i, col := range cols {
					var v interface{}
					_, v = exifData.Expr(col)
					outCols[i] = fmt.Sprintf("%v", v)
				}

			} else {
				for i, col := range cols {
					outCols[i], _ = exifData.Expr(col)
				}
			}
			csvW.Write(outCols)
			csvW.Flush()
		case "detail":
			w.WriteString(exifData.String())
		case "json":
			w.WriteString(exifData.Json())
		case "keys":
			for _, key := range exifData.Keys() {
				w.WriteString(fmt.Sprintln(key))
			}
		}
	}
}
