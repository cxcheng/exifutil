package exifutil

import (
	"fmt"
	"math/rand"
	"runtime/debug"
	"testing"
	"time"

	exifutil "github.com/cxcheng/exifutil"
	exifcommand "github.com/cxcheng/exifutil/command"
)

var conf exifcommand.Config

func TestMain(m *testing.M) {
	conf = exifcommand.Config{
		ExitOnFirstError: false,
		FileExts:         []string{},
		Tags:             []string{},
		LogPath:          "exif.log",
		Trim:             true,
		Tz:               "Asia/Singapore",
		Verbose:          true,
	}

}

func AssertTrue(test *testing.T, cond bool) {
	if !cond {
		test.Error("Failed to test condition")
		debug.PrintStack()
	}
}

func TestParseDate(t *testing.T) {
	// Good dates
	for i := 0; i < 50; i++ {
		year := rand.Intn(100) + 1980
		month := rand.Intn(12) + 1
		day := rand.Intn(28) + 1
		hour := rand.Intn(24)
		min := rand.Intn(60)
		sec := rand.Intn(60)
		subsec := rand.Intn(200)
		s := fmt.Sprintf("%04d:%02d:%02d %02d:%02d:%02d", year, month, day, hour, min, sec)

		var tm time.Time
		var err error
		if tm, err = exifutil.ParseDate(s, subsec, nil); err != nil {
			t.Error(err)
		}
		AssertTrue(t, tm.Year() == year)
		AssertTrue(t, tm.Month() == time.Month(month))
		AssertTrue(t, tm.Day() == day)
		AssertTrue(t, tm.Hour() == hour)
		AssertTrue(t, tm.Minute() == min)
		AssertTrue(t, tm.Second() == sec)
		println(subsec)
		println(tm.Nanosecond())
		AssertTrue(t, tm.Nanosecond() == subsec)
	}
}
