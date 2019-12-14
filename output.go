package exifutil

import (
	"encoding/csv"
	"os"
)

type MetadataOutput struct {
	in  PipelineChan
	out PipelineChan

	cols       []string
	path       string
	outType    string
	tagsToLoad []string
}

func (c *MetadataOutput) Init(config *Config) error {
	c.cols = config.Output.Cols
	c.tagsToLoad = config.Input.TagsToLoad

	// Setup default output columns
	if len(c.cols) == 0 {
		c.cols = []string{"FileName"}
	}

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
	// Setup output file
	var w *os.File
	var err error
	if w, err = os.Create(c.path); err != nil {
		// Substitute with Stdout
		w = os.Stdout
	}
	defer func() { w.Close() }() // close file on exit

	// Setup for CSV if specified, or multi-cols
	var csvW *csv.Writer
	if c.outType == "csv" {
		csvW = csv.NewWriter(w)

		// Output headers
		csvW.Write(c.cols)
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
				outCols := make([]string, len(c.cols))
				for i, col := range c.cols {
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
