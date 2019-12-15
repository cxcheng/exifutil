package exifutil

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/Knetic/govaluate"
	"gopkg.in/yaml.v3"
)

var convertToNumberSuffix = []string{" mm", " m"}

type MetadataConfig struct {
	ExiftoolPath string            `yaml:"exiftool"`
	NameMap      map[string]string `yaml:"map"`
	SubSecDate   map[string]string `yaml:"subsec_date"`
	Remove       []string          `yaml:"remove"`
}

type Metadata struct {
	Path string
	V    map[string]interface{}
}

type MetadataReader struct {
	config     *MetadataConfig
	removeList []*regexp.Regexp
	et         *Exiftool
}

func MakeMetadataReader(configPath string) (*MetadataReader, error) {
	var err error

	reader := MetadataReader{}

	config := new(MetadataConfig)
	var f *os.File
	if f, err = os.Open(configPath); err == nil {
		decoder := yaml.NewDecoder(f)
		if err = decoder.Decode(config); err != nil {
			log.Printf("[Metadata] Error reading config [%s]: %s", configPath, err)
		}
		defer f.Close() // close immediately after exiting this
	}
	reader.config = config

	// Initialize external exiftool
	etInitFunc := func(et *Exiftool) error {
		if config.ExiftoolPath != "" {
			et.Binary = config.ExiftoolPath
		}
		et.InitArgs = append(et.InitArgs, "-n")
		return nil
	}
	if reader.et, err = NewExiftool(etInitFunc); err != nil {
		log.Printf("[Metadata] Error intializing MetadataReader: %s", err)
		return nil, err
	}

	// Process the regexp
	reader.removeList = make([]*regexp.Regexp, 0, len(config.Remove))
	for _, pattern := range config.Remove {
		regex, err := regexp.Compile(pattern)
		if err != nil {
			return nil, fmt.Errorf("[Metadata] Error parsing [%s]: %v", pattern, err)
		}
		reader.removeList = append(reader.removeList, regex)
	}
	return &reader, nil
}

func (r *MetadataReader) Close() {
	if r.et != nil {
		r.et.Close()
		r.et = nil
	}
}

func parseDate(s string, tz *time.Location) interface{} {
	// Try different approaches to parsing
	if t, err := time.Parse("2006:01:02 15:04:05", s); err == nil {
		return t.In(tz)
	} else if t, err := time.Parse("2006:01:02 15:04:05.999999999", s); err == nil {
		return t.In(tz)
	} else if t, err := time.Parse("2006:01:02 15:04:05Z07:00", s); err == nil {
		return t
	} else if t, err := time.Parse("2006:01:02 15:04:05.999999999Z07:00", s); err == nil {
		return t
	} else {
		return time.Time{} // return 0 time if nothing can be found
	}
}

func (r *MetadataReader) ReadMetadata(paths []string, tz *time.Location, tagsToLoadMap map[string]bool) ([]Metadata, error) {
	metas := r.et.ExtractMetadata(paths...)
	results := make([]Metadata, 0, len(metas))
	for _, meta := range metas {
		result := Metadata{
			Path: meta.File,
			V:    make(map[string]interface{}),
		}
		for k, v := range meta.Fields {
			// Translate/transform common fields with different field anems
			if k2, found := r.config.NameMap[k]; found {
				if k2 == "" {
					// Skip the field because it maps to empty
					continue
				}
				// check that we are not overwriting mapped
				if _, found := result.V[k2]; found {
					continue
				}
				k = k2
			}

			// Skip fields that are not specified in the include list
			if len(tagsToLoadMap) > 0 {
				if _, found := tagsToLoadMap[k]; !found {
					// Skip if it is not on the include list
					continue
				}
			}

			if s, ok := v.(string); ok {
				// Adjust some string encoded values to binary form
				v = strings.TrimSpace(s) // trim spaces first
				if len(s) == 0 || strings.HasPrefix(s, "(Binary data") {
					// Skip binary or empty content
					continue
				} else if k2, found := r.config.SubSecDate[k]; found {
					k = k2
					v = parseDate(s, tz)
					// Update new value as k2, but remove original k in post-process pass
				} else if strings.Contains(k, "Date") {
					// If key contains Date, try to convert to time.Time
					v = parseDate(s, tz)
				}
			} else if f64, ok := v.(float64); ok {
				const maxUint16 = float64(^uint16(0))
				const maxInt32 = float64(int(^uint32(0) >> 1))
				const minInt32 = float64(-maxInt32 - 1)

				// Convert to integer if no fractional part
				if f64 == math.Trunc(f64) {
					if f64 >= 0 && f64 < maxUint16 {
						v = uint16(f64)
					} else if f64 > minInt32 && f64 < maxInt32 {
						v = int32(f64)
					} else {
						v = int64(f64)
					}
				}
			}

			// Skip those on the remove list. Can only do that after subsec processing
			matchedRemoveK := false
			for _, removeRegexp := range r.removeList {
				if matchedRemoveK = removeRegexp.MatchString(k); matchedRemoveK {
					//log.Printf("Skipping tag [%s] -> [%s]", removeRegexp, k)
					break
				}
			}
			if matchedRemoveK {
				continue
			}

			// Adjust specific keys
			switch k {
			case "SerialNumber":
				v = fmt.Sprintf("%v", v)
			}

			//fmt.Printf("%s[%v] %v    - %s\n", meta.File, k, v, reflect.TypeOf(v))
			result.V[k] = v
		}

		// Post-process to remove subsec fields
		/*
			for k, _ := range r.config.SubSecDate {
				if _, found := result.V[k]; found {
					delete(result.V, k)
				}
			}
		*/

		// Add to results
		results = append(results, result)
	}
	return results, nil
}

func (d *Metadata) ConstructKey() string {
	// Compute unique key hash
	ts, found := d.V["DateTimeOriginal"]
	if !found {
		ts = time.Time{}
	}
	key := fmt.Sprintf("%s%s%s%s",
		d.V["ImageUniqueID"], d.V["FileName"], d.V["SerialNumber"], ts)

	const (
		p uint64 = 31
		m uint64 = 4710343600320809
	)
	var hash uint64 = 0
	var pow uint64 = 1
	for _, c := range key {
		hash = (hash + (uint64(c)-'a'+1)*pow) % m
		pow = (pow * p) % m
	}
	return fmt.Sprintf("%016x", hash)
}

func (d *Metadata) Expr(expr string) interface{} {
	// If it starts and end with a "@", it is an expression
	// If it starts with a "%", it is a template
	// Otherwise, it is a column
	if len(expr) > 0 {
		if expr[0] == '@' {
			return d.Eval(expr[1:])
		} else if expr[0] == '%' {
			expr2 := expr[1:]
			rsBuf := bytes.NewBufferString("")
			state, pos := 0, 0
			for i := 0; i < len(expr2); i++ {
				c := rune(expr2[i])
				switch state {
				case 0: // normal, ready
					if c == '[' {
						state, pos = 1, i+1
					} else {
						rsBuf.WriteRune(c)
					}
				case 1: // waiting for ']' to close, or end of expr
					if c == ']' || i >= (len(expr2)-1) {
						rsBuf.WriteString(d.ExprString(expr2[pos:i]))
						state = 0
					}
				}
			}
			return rsBuf.String()
		} else {
			// Get tag value directly
			if result, found := d.V[expr]; found {
				return result
			} else {
				return ""
			}
		}
	}
	return ""
}

func (d *Metadata) Eval(expr string) interface{} {
	var result interface{}
	var err error
	if len(expr) > 0 {
		var eval *govaluate.EvaluableExpression
		if eval, err = govaluate.NewEvaluableExpression(expr); err == nil {
			if result, err = eval.Evaluate(d.V); err != nil {
				log.Printf("Error evaluating [%s]: %v", expr, err)
				result = ""
			}
		}
	}
	return result
}

func (d *Metadata) ExprString(expr string) string {
	return fmt.Sprintf("%v", d.Expr(expr))
}

func (d *Metadata) Json() string {
	if b, err := json.Marshal(d.V); err != nil {
		return ""
	} else {
		return string(b)
	}
}
