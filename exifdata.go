package exifutil

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
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

func parseInt(s string) (int, error) {
	n, err := strconv.ParseInt(s, 10, 64)
	return int(n), err
}

func ParseRational(s string) float64 {
	var n1, n2 int
	var err error

	startPos := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '/' {
			if n1, err = parseInt(s[startPos:i]); err != nil {
				return 0.0
			}
			startPos = i + 1
		}
	}
	if n2, err = parseInt(s[startPos:]); err != nil {
		return float64(n1)
	} else {
		return float64(n1) / float64(n2)
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

func (d *ExifData) add(tagsToInclude []string, tagId uint16, tagPath string, tagType uint16, s string, v interface{}) {
	if addToResults(tagsToInclude, tagPath) {
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
			log.Printf("Skipping missing tagId [%s]: [%s:0x%04x]\n", exifPath, ifdPath, tagId)
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
		s, err1 := tagType.ResolveAsString(valueContext, true)
		v, err2 := tagType.Resolve(valueContext)
		if err1 != nil || err2 != nil {
			log.Printf("Tag [%s] parsing error: %s %s", err1, err2)
		} else {
			if trim {
				s = strings.TrimSpace(s)
			}

			// Skip tag if it is not on the specified list; filter out empty tags
			exifData.add(tagsToLoad, tagId, tagPath, tagType.Type(), s, v)

		}

		return nil
	}

	if _, err = exif.Visit(exif.IfdStandard, im, ti, rawExif, visitor); err == nil {
		// Post-process DateTime tags
		for dtTagName, subSecTagName := range dateTimeTags {
			if e, found := exifData.entries[dtTagName]; found {
				var subsec int = 0
				if v2, found := exifData.values[subSecTagName]; found {
					// for some reason, the SubSecTime* tags have ASCII type, need Atoi
					if v2str, ok := v2.(string); ok {
						subsec, err = parseInt(v2str)
					} else if subsec64, ok := v2.(int64); ok {
						subsec = int(subsec64)
					}
					// update to int64
					exifData.values[subSecTagName] = int64(subsec)
				}
				var tv time.Time
				if tv, err = ParseDate(e.s, subsec, loc); err == nil {
					exifData.values[dtTagName] = tv.UnixNano() // replace with Linux timevalue
				}
			}
		}
		// Insert file info
		exifData.add(tagsToLoad, 0, "Sys/Name", exif.TypeAscii, filepath.Base(exifPath), filepath.Base(exifPath))
		exifData.add(tagsToLoad, 0, "Sys/Len", exif.TypeLong, fmt.Sprintf("%d", finfo.Size()), uint64(finfo.Size()))
		exifData.add(tagsToLoad, 0, "Sys/Path", exif.TypeAscii, exifPath, exifPath)
		exifData.add(tagsToLoad, 0, "Sys/Tz", exif.TypeAscii, loc.String(), loc.String())
		tv := finfo.ModTime()
		exifData.add(tagsToLoad, 0, "Sys/Mod", exif.TypeAscii, tv.String(), tv)

		// Insert unique key
		uniqueKey := exifData.Get("ImageUniqueID")
		if uniqueKey == "" {
			h := fmt.Sprintf("%s%s%s%s", filepath.Base(exifPath), exifData.Get("Make"), exifData.Get("Model"), exifData.Get("DateTimeOriginal"))
			uniqueKey = computeHash(h)
		} else {
			uniqueKey = strings.TrimRight(uniqueKey, "0")
		}
		exifData.add(tagsToLoad, 0, "Sys/Key", exif.TypeAscii, uniqueKey, uniqueKey)
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
