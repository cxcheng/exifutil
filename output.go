package exifutil

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"reflect"
)

type MetadataOutput struct {
	in  PipelineChan
	out PipelineChan

	cols       []string
	keys       bool
	path       string
	outType    string
	tagsToLoad []string
}

func (c *MetadataOutput) Init(config *Config) error {
	c.cols = config.Output.Cols
	c.keys = config.Output.Keys
	c.path = config.Output.Path
	c.outType = config.Output.Type
	c.tagsToLoad = config.Input.TagsToLoad

	// Setup default output columns
	if c.keys {
		c.cols = []string{"Tag", "Count", "Type"}
	} else if len(c.cols) == 0 {
		c.cols = []string{"FileName"}
	}

	// Check and set up output type
	switch c.outType {
	case "":
		c.outType = "csv"
	case "csv", "json":
		break
	default:
		return fmt.Errorf("[Output]: Unknown output type %s", c.outType)
	}

	// Add output extension if not provided
	if c.path != "" && filepath.Ext(c.path) == "" {
		c.path = fmt.Sprintf("%s.%s", c.path, c.outType)
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
	// Return if no output
	if len(c.cols) == 0 {
		return fmt.Errorf("[Output]: No output columns specified, nothing to output")
	}

	// Setup output file
	var w *os.File
	var err error
	if len(c.path) > 0 {
		if w, err = os.Create(c.path); err != nil {
			log.Printf("[Output]: Error creating output [%s]: %v", c.path, err)
		}
	}
	if w == nil {
		// Set to stdout if no output setup yet
		w = os.Stdout
	}
	defer func() { w.Close() }() // close file on exit

	// Setup for CSV or keys
	var csvW *csv.Writer
	if c.outType == "csv" {
		csvW = csv.NewWriter(w)

		// Output headers
		csvW.Write(c.cols)
	}

	type keysMapEntry struct {
		count int
		typeV reflect.Type
	}
	var keysMap map[string]keysMapEntry = nil
	if c.keys {
		keysMap = make(map[string]keysMapEntry)
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
			if c.keys {
				for k, v := range md.V {
					if e, found := keysMap[k]; found {
						keysMap[k] = keysMapEntry{count: e.count + 1, typeV: reflect.TypeOf(v)}
					} else {
						keysMap[k] = keysMapEntry{count: 1, typeV: reflect.TypeOf(v)}
					}
				}
			} else {
				switch c.outType {
				case "csv":
					outCols := make([]string, len(c.cols))
					for i, col := range c.cols {
						outCols[i] = md.ExprString(col)
					}
					csvW.Write(outCols)
					csvW.Flush()
				case "json":
					w.WriteString(fmt.Sprintf("%s\n", md.Json()))
				default:
					return fmt.Errorf("[Output]: Unknown type [%s]", c.outType)
				}
			}
		}
	}

	// Output keys
	if c.keys {
		for k, e := range keysMap {
			if csvW != nil {
				csvW.Write([]string{k, fmt.Sprintf("%d", e.count), fmt.Sprintf("%v", e.typeV)})
			} else {
				w.WriteString(fmt.Sprintf("%s\n", k))
			}
		}
	}
	// Need to flush or not everything buffered will be returned
	if csvW != nil {
		csvW.Flush()
	}

	return nil
}
