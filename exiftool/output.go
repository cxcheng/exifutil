package exiftool

import (
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/cxcheng/exifutil"
)

type OutputContext struct {
	conf       *Config
	in         chan *exifutil.ExifData
	callOnExit func(time.Time, string)

	cols      []string
	csv       bool
	outType   string
	useValues bool
}

func MakeOutput(conf *Config, callOnExit func(time.Time, string), in chan *exifutil.ExifData) (*OutputContext, error) {
	defer callOnExit(time.Now(), "Init DB")

	var ctx OutputContext
	var err error

	ctx = OutputContext{conf: conf, callOnExit: callOnExit, in: in}

	// Process command-line arguments
	var cols string
	flag.StringVar(&cols, "cols", "", "Columns to output")
	flag.StringVar(&ctx.outType, "type", "", "Output type: csv, json, keys")
	flag.BoolVar(&ctx.useValues, "values", false, "Output value instead of original text")
	flag.Parse()

	// Setup default output columns
	if cols == "" {
		if len(conf.Input.TagsToLoad) > 0 {
			ctx.cols = conf.Input.TagsToLoad
		} else {
			ctx.cols = []string{"Sys/Name", "Sys/Key", "Make", "Model", "DateTimeOriginal"}
		}
	} else {
		ctx.cols = strings.Split(cols, ",")
	}

	// Setup output type
	if ctx.outType == "" {
		ctx.outType = "csv"
	}

	return &ctx, err
}

func (ctx *OutputContext) Output(wg *sync.WaitGroup) {
	defer ctx.callOnExit(time.Now(), "Output")

	var w *os.File
	var err error
	if w, err = os.Create(ctx.conf.Output.Path); err != nil {
		// Substitute with Stdout
		w = os.Stdout
	}

	defer func() { w.Close() }() // close file on exit

	// Setup for CSV if specified, or multi-cols
	var csvW *csv.Writer
	if ctx.outType == "csv" {
		csvW = csv.NewWriter(w)
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
			outCols := make([]string, len(ctx.cols))
			if ctx.useValues {
				for i, col := range ctx.cols {
					var v interface{}
					_, v = exifData.Expr(col)
					outCols[i] = fmt.Sprintf("%v", v)
				}

			} else {
				for i, col := range ctx.cols {
					outCols[i], _ = exifData.Expr(col)
				}
			}
			csvW.Write(outCols)
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
	wg.Done()
}
