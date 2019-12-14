package exifutil

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/big"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/Knetic/govaluate"
	"github.com/barasher/go-exiftool"
	"gopkg.in/yaml.v3"
)

var convertToNumberSuffix = []string{" mm", " m"}

type MetadataConfig struct {
	NameMap    map[string]string `yaml:"map"`
	SubSecDate map[string]string `yaml:"subsec_date"`
}

type Metadata struct {
	Path string
	V    map[string]interface{}
}

type MetadataReader struct {
	config *MetadataConfig
	et     *exiftool.Exiftool
	stats  map[string]interface{}
}

func MakeMetadataReader(configPath string) (*MetadataReader, error) {
	var err error

	reader := MetadataReader{}

	config := new(MetadataConfig)
	var f *os.File
	if f, err = os.Open(configPath); err == nil {
		decoder := yaml.NewDecoder(f)
		if err = decoder.Decode(config); err != nil {
			log.Printf("Error reading config [%s]: %s", configPath, err)
		}
		defer f.Close() // close immediately after exiting this
	}
	reader.config = config

	if reader.et, err = exiftool.NewExiftool(); err != nil {
		log.Printf("Error intializing MetadataReader: %s", err)
		return nil, err
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

func computeUniqeKey(s string) string {
	const (
		p uint64 = 31
		m uint64 = 4710343600320809
	)
	var hash uint64 = 0
	var pow uint64 = 1
	for _, c := range s {
		hash = (hash + (uint64(c)-'a'+1)*pow) % m
		pow = (pow * p) % m
	}
	return fmt.Sprintf("%016x", hash)
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
				} else if strings.Contains(s, "/") {
					// Try to convert rational numbers
					if unicode.IsDigit(rune(s[0])) {
						r := new(big.Rat)
						if _, err := fmt.Sscan(s, r); err == nil {
							v2, _ := r.Float64()
							// Add a .v field
							result.V[k+".v"] = v2
						}
					}
				} else {
					// Try to convert to numbers for selected units
					for _, suffix := range convertToNumberSuffix {
						if strings.HasSuffix(s, suffix) {
							// Try both int and float, return int if no fractional part
							t := s[:len(suffix)]
							if v2, err := strconv.ParseInt(t, 10, 32); err == nil {
								v = v2
								break
							} else if v2, err := strconv.ParseFloat(t, 64); err == nil {
								v = v2
								break
							}
						}
					}
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

			// Adjust serial number to string
			if k == "SerialNumber" {
				v = fmt.Sprintf("%v", v)
			}

			fmt.Printf("%s[%v] %v    - %s\n", meta.File, k, v, reflect.TypeOf(v))
			result.V[k] = v
		}

		// Post-process to remove subsec fields
		for k, _ := range r.config.SubSecDate {
			if _, found := result.V[k]; found {
				delete(result.V, k)
			}
		}

		// Compute unique key hash
		result.V["Key"] = computeUniqeKey(fmt.Sprintf("%s%s%s%s",
			result.V["ImageUniqueID"], result.V["FileName"], result.V["SerialNumber"], result.V["DateTimeOriginal"]))

		// Add to results
		results = append(results, result)
	}
	return results, nil
}

func (d *Metadata) Expr(expr string) interface{} {
	// If it starts and end with a "@", it is an expression
	// If it starts with a "%", it is a template
	// Otherwise, it is a column
	var result interface{}
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
						d.expandTag(rsBuf, expr2[pos:i])
						state = 0
					}
				}
			}
			return rsBuf.String()
		} else {
			// Get tag value directly
			if result, found := d.V[expr]; found {
				return result
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
			var rs interface{}
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
	return json.Marshal(d.V)
}
