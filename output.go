package exifutil

import (
	"encoding/csv"
	"os"
	"strings"
)

type MetadataOutput struct {
	in  PipelineChan
	out PipelineChan

	colsArg    string
	outPath    string
	outPathArg string
	outType    string
	tagsToLoad []string
}

func (c *MetadataOutput) Init(config *Config) error {
	c.tagsToLoad = config.Input.TagsToLoad

	// Setup output type
	if c.outType == "" {
		c.outType = "csv"
	}

	return nil
}

func (c *MetadataOutput) SetInput(in PipelineChan) {
	c.in = in
}

func (c *MetadataOutput) SetOutput(out PipelineChan) {
	c.out = out
}

func (c *MetadataOutput) Run() error {
	// Setup default output columns
	var cols []string
	if c.colsArg == "" {
		if len(c.tagsToLoad) > 0 {
			cols = c.tagsToLoad
		} else {
			cols = []string{"FileName", "Key", "Make", "Model", "DateTimeOriginal"}
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
		o := <-c.in
		if c.out != nil {
			// forward to next stage if there is one
			c.out <- o
		}
		if o == nil || o.err != nil {
			// No more inputs, exit
			break
		}
		for _, md := range o.data {
			switch c.outType {
			case "csv":
				outCols := make([]string, len(cols))
				for i, col := range cols {
					outCols[i] = md.ExprString(col)
				}
				csvW.Write(outCols)
				csvW.Flush()
			case "json":
				w.WriteString(md.Json())
			}

		}
	}

	return nil
}
