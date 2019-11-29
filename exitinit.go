package exifutil

import (
	"time"
)

var defaultLoc *time.Location
var ipdPathsToStrip = [...]string{"IFD", "IFD/Exif", "IFD/GPSInfo"}
var dateTimeTags = map[string]string{
	"DateTime":          "SubSecTime",
	"DateTimeOriginal":  "SubSecTimeOriginal",
	"DateTimeDigitized": "SubSecTimeDigitized",
}

func init() {
	// initialize defaultLoc
	var err error
	defaultLoc, err = time.LoadLocation("Etc/UTC")
	if err != nil {
		defaultLoc = time.Now().Location()
	}
}
