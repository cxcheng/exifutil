package exifutil

import (
	"encoding/csv"
	"errors"
	"fmt"
	"log"
	"os"
	"sort"
	"time"
)

type Command struct {
	OutPath string
	OutType string

	Filter        string
	Cols          []string
	OrderBy       string
	OrderReversed bool
	TagsToLoad    []string
	Tz            *time.Location
}

type Output struct {
	w         *os.File
	csvW      *csv.Writer
	Headings  []string
	Records   []Record
	sortByCol int
}

type Record struct {
	cols  []string
	colVs []interface{}
}

func (o *Output) Init(command *Command) error {
	var f *os.File
	var err error
	if command.OutPath != "" {
		if o.w, err = os.OpenFile(command.OutPath, os.O_RDWR|os.O_CREATE, 0666); err == nil {
			defer f.Close()
		}
	}
	if o.w == nil {
		// default to stdout
		o.w = os.Stdout
	}
	if command.OutType == "csv" {
		if len(command.Cols) == 0 {
			return errors.New("No columns specified for CSV using --csv")
		}
		o.csvW = csv.NewWriter(o.w)
	}
	for _, heading := range command.Cols {
		o.Headings = append(o.Headings, heading)
	}
	o.sortByCol = -1
	if command.OrderBy != "" {
		// Check if ordering by specified tag
		for i, tag := range command.Cols {
			if command.OrderBy == tag {
				o.sortByCol = i
				break
			}
		}
	}

	return nil
}

func (o *Output) Flush() {
	if o.csvW != nil {
		o.csvW.Flush()
	}
}

func (o *Output) OutHeading() {
	if o.csvW != nil {
		o.csvW.Write(o.Headings)
	}
}

func (o *Output) Out(s string) {
	o.OutRecord(Record{
		cols:  []string{s},
		colVs: []interface{}{s},
	})
}

func (o *Output) OutRecord(r Record) {
	if o.sortByCol >= 0 {
		// buffer
		o.Records = append(o.Records, r)
	} else if o.csvW != nil {
		o.csvW.Write(r.cols)
	} else {
		// only 1 column written if not in CSV mode
		fmt.Fprintln(o.w, r.cols[0])
	}
}

func (o *Output) OutBuffered() {
	// Sort
	if o.sortByCol >= 0 && o.sortByCol < len(o.Headings) {
		sort.Sort(o)
		o.sortByCol = -1 // mark as done
	}

	// Output
	for _, record := range o.Records {
		o.OutRecord(record)
	}
	o.Flush()
}

func (o *Output) Len() int {
	return len(o.Records)
}

func (o *Output) Less(i, j int) bool {
	success := false
	vi := o.Records[i].colVs[o.sortByCol]
	//fmt.Printf("###### %d %d %v\n", i, j, vi)
	switch vi.(type) {
	case string:
		if vj, ok := o.Records[j].colVs[o.sortByCol].(string); ok {
			success = vi.(string) < vj
		}
	case float64:
		if vj, ok := o.Records[j].colVs[o.sortByCol].(float64); ok {
			success = vi.(float64) < vj
		}
	case int32:
		if vj, ok := o.Records[j].colVs[o.sortByCol].(int32); ok {
			success = vi.(int32) < vj
		}
	}
	return success
}

func (o *Output) Swap(i, j int) {
	o.Records[i], o.Records[j] = o.Records[j], o.Records[i]
}

func Process(path string, f os.FileInfo, err error, command *Command, out *Output) error {
	exifData, err := ReadExifData(path, command.Tz, command.TagsToLoad)
	if err != nil {
		log.Fatalf("[%s]: [%s]\n", path, err)
	} else {
		// Apply filter
		if command.Filter == "" || exifData.Filter(command.Filter) {
			// Output
			switch command.OutType {
			case "json":
				out.Out(exifData.Json())
			case "keys":
				out.sortByCol = 0
				for _, key := range exifData.Keys() {
					out.Out(key)
				}
			default:
				// if cols specified, evaluate; otherwise, print every field
				if len(command.Cols) > 0 {
					outCols := make([]string, len(command.Cols))
					outColVs := make([]interface{}, len(command.Cols))
					for i, col := range command.Cols {
						outCols[i], outColVs[i] = exifData.Expr(col)
					}
					out.OutRecord(Record{
						cols:  outCols,
						colVs: outColVs,
					})
				} else {
					out.Out(exifData.String())
				}
			}
		}
	}
	return err
}
