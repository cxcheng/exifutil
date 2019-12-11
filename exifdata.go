package exifutil

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Knetic/govaluate"
	"github.com/dsoprea/go-exif"
)

type ExifEntry struct {
	id uint16
	s  string
	t  uint16
}

func (e ExifEntry) MarshalJSON() ([]byte, error) {
	return json.Marshal(e.s)
}

type ExifData struct {
	entries map[string]*ExifEntry
	values  map[string]interface{}
}

func ParseDate(dateString string, subsec int, loc *time.Location) (time.Time, error) {
	// Parse for the format: YYYY:mm:DD HH:MM:SS
	// Optionally add timezone

	if subsec > 0 {
		dateString = fmt.Sprintf("%s.%d", dateString, subsec)
	}
	if t, err := time.Parse("2006:01:02 15:04:05.99", dateString); err != nil {
		return time.Time{}, err
	} else {
		return t.In(loc), nil
	}
}

func ReadExifData(exifPath string, loc *time.Location, trim bool, tagsToLoad []string) (*ExifData, error) {
	f, err := os.Open(exifPath)
	if err != nil {
		return nil, err
	}
	data, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, err
	}
	finfo, err := f.Stat()
	return MakeExifData(exifPath, finfo, data, loc, trim, tagsToLoad)
}

func makeExifEntry(id uint16, s string, t uint16) *ExifEntry {
	e := new(ExifEntry)
	e.id, e.s, e.t = id, s, t
	return e
}

func addToResults(tags []string, tagPath string) bool {
	// Skip tag if it is not on the specified list; filter out empty tags
	if tags != nil && len(tags) > 0 {
		tagNotFound := true
		someTagsNotEmpty := false
		for _, tagToReturn := range tags {
			if tagPath == tagToReturn {
				tagNotFound = false
				break
			}
			if len(tagToReturn) > 0 {
				someTagsNotEmpty = true
			}
		}
		if tagNotFound && someTagsNotEmpty {
			return false
		}
	}
	return true
}

func computeHash(s string) string {
	const (
		p uint64 = 31
		m uint64 = 100000009
	)
	var hash uint64 = 0
	var pow uint64 = 1
	for _, c := range s {
		hash = (hash + (uint64(c)-'a'+1)*pow) % m
		pow = (pow * p) % m
	}
	return fmt.Sprintf("%016x", hash)
}

func (d *ExifData) add(tagsToInclude []string, tagId uint16, tagPath string, tagType uint16, s string, v interface{}, trim bool) {
	if addToResults(tagsToInclude, tagPath) {
		// try trimming
		if trim {
			if s2, ok := v.(string); ok {
				s = strings.TrimSpace(s)
				v = strings.TrimSpace(s2)
			}
		}
		e := makeExifEntry(tagId, s, tagType)
		d.entries[tagPath], d.values[tagPath] = e, v
	}
}

func MakeExifData(exifPath string, finfo os.FileInfo, data []byte, loc *time.Location, trim bool, tagsToLoad []string) (*ExifData, error) {

	exifData := ExifData{
		entries: make(map[string]*ExifEntry),
		values:  make(map[string]interface{}),
	}

	// Parse content to extract EXIF data

	rawExif, err := exif.SearchAndExtractExif(data)
	if err != nil {
		return nil, err
	}

	// Run the parse.

	im := exif.NewIfdMappingWithStandard()
	ti := exif.NewTagIndex()

	visitor := func(fqIfdPath string, ifdIndex int, tagId uint16, tagType exif.TagType, valueContext exif.ValueContext) error {
		defer func() {
			if state := recover(); state != nil {
				//log.Printf("%v", state)
			}
		}()

		var err error // any error encountered

		// Obtain the path and tag names
		var ifdPath string
		if ifdPath, err = im.StripPathPhraseIndices(fqIfdPath); err != nil {
			return err
		}
		var it *exif.IndexedTag
		if it, err = ti.Get(ifdPath, tagId); err != nil {
			log.Printf("[%s] Skipping missing tag [%s]: 0x%04x", exifPath, ifdPath, tagId)
			return nil // TO DO: should not be the case, but we need to ignore it
		}

		// Compute tag path; exclude full path if it is part of the standard paths
		var tagPath string
		useFullPath := true
		for _, ipdPathToStrip := range ipdPathsToStrip {
			if ipdPathToStrip == ifdPath {
				useFullPath = false
				break
			}
		}
		if useFullPath {
			tagPath = fmt.Sprintf("%s/%s", ifdPath, it.Name)
		} else {
			tagPath = it.Name
		}

		// Compute the values
		if tagType.Type() != exif.TypeUndefined {
			s, err1 := tagType.ResolveAsString(valueContext, true)
			v, err2 := tagType.Resolve(valueContext)
			if err1 != nil {
				log.Printf("[%s] tag [%s] parsing error: %s", exifPath, tagPath, err1)
			} else if err2 != nil {
				log.Printf("[%s] tag [%s] parsing error: %s", exifPath, tagPath, err2)
			} else {
				if trim {
					s = strings.TrimSpace(s)
				}

				// Skip tag if it is not on the specified list; filter out empty tags
				exifData.add(tagsToLoad, tagId, tagPath, tagType.Type(), s, v, trim)
			}
		} else {
			var s string
			if len(valueContext.AddressableData) > 16 {

				s = hex.Dump(valueContext.AddressableData[:16])
			} else {
				s = hex.Dump(valueContext.AddressableData)
			}

			log.Printf("[%s] Skipping [%s] undefined type: %d\n    %v", exifPath, tagPath, valueContext.UnitCount, s)

		}

		return nil
	}

	if _, err = exif.Visit(exif.IfdStandard, im, ti, rawExif, visitor); err == nil {
		// Post-process DateTime tags from pre-defined list of tags
		for dtTagName, subSecTagName := range dateTimeTags {
			if e, found := exifData.entries[dtTagName]; found {
				// For each DateTime tag, update with subsec tag if it exists
				var subsecNano int
				if subsecV, found := exifData.values[subSecTagName]; found {
					// SubSecTime* tags have ASCII type, and are stored as ASCII fractions
					if s, ok := subsecV.(string); ok {
						var f float64
						if _, err := fmt.Sscanf("0."+s, "%f", &f); err == nil {
							subsecNano = int(f * 1000000000)
						}
						exifData.values[subSecTagName] = subsecNano
					}
				}
				var tv time.Time
				if tv, err = ParseDate(e.s, subsecNano, loc); err == nil {
					exifData.values[dtTagName] = tv
				}
			}
		}
		// Insert file info
		exifData.add(tagsToLoad, 0, "Sys/Name", exif.TypeAscii, filepath.Base(exifPath), filepath.Base(exifPath), false)
		exifData.add(tagsToLoad, 0, "Sys/Len", exif.TypeLong, fmt.Sprintf("%d", finfo.Size()), uint64(finfo.Size()), false)
		exifData.add(tagsToLoad, 0, "Sys/Path", exif.TypeAscii, exifPath, exifPath, false)
		exifData.add(tagsToLoad, 0, "Sys/Tz", exif.TypeAscii, loc.String(), loc.String(), false)
		tv := finfo.ModTime()
		exifData.add(tagsToLoad, 0, "Sys/Mod", exif.TypeAscii, tv.String(), tv, false)

		// Insert unique key
		uniqueKey := exifData.Get("ImageUniqueID")
		if uniqueKey == "" {
			h := fmt.Sprintf("%s%s%s%s", filepath.Base(exifPath), exifData.Get("Make"), exifData.Get("Model"), exifData.Get("DateTimeOriginal"))
			uniqueKey = computeHash(h)
		} else {
			uniqueKey = strings.TrimRight(uniqueKey, "0")
		}
		exifData.add(tagsToLoad, 0, "Sys/Key", exif.TypeAscii, uniqueKey, uniqueKey, false)
	}
	return &exifData, err
}

func (d *ExifData) expandTag(rsBuf *bytes.Buffer, tagPath string) {
	if len(tagPath) > 0 {
		if e, ok := d.entries[tagPath]; ok {
			rsBuf.WriteString(e.s)
		} else {
			rsBuf.WriteString(fmt.Sprintf("[%s]", tagPath))
		}
	}
}

func (d *ExifData) Expr(expr string) (string, interface{}) {
	// If it starts and end with a "@", it is an expression
	// If it starts with a "%", it is a template
	// Otherwise, it is a column
	if len(expr) > 0 {
		if expr[0] == '@' {
			if rs, err := d.Eval(expr[1:]); err == nil {
				return fmt.Sprintf("%v", rs), rs
			}
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
			return rsBuf.String(), rsBuf.String()
		} else {
			// Get tag value directly
			if e, ok := d.entries[expr]; ok {
				if v, ok := d.values[expr]; ok {
					return e.s, v
				} else {
					return e.s, e.s
				}
			}
		}
	}
	return "", ""
}

func (d *ExifData) Eval(expr string) (interface{}, error) {
	var err error
	if len(expr) > 0 {
		var eval *govaluate.EvaluableExpression
		if eval, err = govaluate.NewEvaluableExpression(expr); err == nil {
			var rs interface{}
			if rs, err = eval.Evaluate(d.values); err == nil {
				return rs, nil
			}
		}
	}
	return false, err
}

func (d *ExifData) Filter(expr string) bool {
	if rs, err := d.Eval(expr); err == nil {
		switch rs.(type) {
		case bool:
			return rs.(bool)
		case string:
			return len(rs.(string)) > 0
		}
	}
	return false
}

func (d *ExifData) Get(tagPath string) string {
	if e, ok := d.entries[tagPath]; ok {
		return e.s
	} else {
		return ""
	}
}

func (d *ExifData) Json() string {
	if b, err := json.Marshal(d.entries); err != nil {
		return ""
	} else {
		return string(b)
	}
}

func (d *ExifData) Keys() []string {
	keys := make([]string, 0, len(d.entries))
	for k, _ := range d.entries {
		keys = append(keys, k)
	}
	return keys
}

func (d *ExifData) String() string {
	var b bytes.Buffer
	for tagPath, entry := range d.entries {
		var typeName string
		var ok bool
		if typeName, ok = exif.TypeNames[entry.t]; !ok {
			typeName = string(entry.t)
		}
		b.WriteString(fmt.Sprintf("%-20s(%04x) [%-8s]: %s (%v)\n", tagPath, entry.id, typeName, entry.s, d.values[tagPath]))
	}
	return b.String()
}

func (d *ExifData) Values() map[string]interface{} {
	return d.values
}
