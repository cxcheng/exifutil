package exifutil

import (
	"testing"

	"github.com/cxcheng/exifutil"
)

func TestParseDate(test *testing.T) {
	goodTimeStrs := []string{
		"1970:01:01 12:34:56",
	}

	for _, goodTimeStr := range goodTimeStrs {
		t, err := exifutil.ParseDate(goodTimeStr, 0, nil)
		println(t.String(), err)
	}
}
