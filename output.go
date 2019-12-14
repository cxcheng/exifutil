package exifutil

import (
	"encoding/csv"
	"fmt"
	"os"
	"strings"
)

type ExifOutput struct {
	in  PipelineChan
	out PipelineChan

	colsArg    string
	outPath    string
	outPathArg string
	outType    string
	tagsToLoad []string
}

func (c *ExifOutput) Init(config *Config) error {
	c.tagsToLoad = config.Input.TagsToLoad

	// Setup output type
	if c.outType == "" {
		c.outType = "csv"
	}

	return nil
}

func (c *ExifOutput) SetInput(in PipelineChan) {
	c.in = in
}

func (c *ExifOutput) SetOutput(out PipelineChan) {
	c.out = out
}

func (c *ExifOutput) Run() error {
	// Setup default output columns
	var cols []string
	if c.colsArg == "" {
		if len(c.tagsToLoad) > 0 {
			cols = c.tagsToLoad
		} else {
			cols = []string{"Sys/Name", "Sys/Key", "Make", "Model", "DateTimeOriginal"}
		}
	} else {
		cols = strings.Split(c.colsArg, ",")
	}

	// Setup output file
	var outPath string
	var w *os.File
	var err error
	if c.outPathArg != "" {
		outPath = c.outPathArg
	} else {
		outPath = c.outPath
	}
	if w, err = os.Create(outPath); err != nil {
		// Substitute with Stdout
		w = os.Stdout
	}
	defer func() { w.Close() }() // close file on exit

	// Setup for CSV if specified, or multi-cols
	var csvW *csv.Writer
	if c.outType == "csv" {
		csvW = csv.NewWriter(w)

		// Output headers
		csvW.Write(cols)
	}

	// Process incoming records
	for {
		exifData := <-c.in
		if c.out != nil {
			// forward to next stage if there is one
			c.out <- exifData
		}
		if exifData == nil {
			// No more inputs, exit
			break
		}
		switch c.outType {
		case "csv":
			outCols := make([]string, len(cols))
			if c.useValues {
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

	return nil
}
