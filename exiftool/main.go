// This tool dumps EXIF information from images.
//
// Example command-line:
//
//   exif-read-tool -filepath <file-path>
//
// Example Output:
//
//   IFD=[IfdIdentity<PARENT-NAME=[] NAME=[IFD]>] ID=(0x010f) NAME=[Make] COUNT=(6) TYPE=[ASCII] VALUE=[Canon]
//   IFD=[IfdIdentity<PARENT-NAME=[] NAME=[IFD]>] ID=(0x0110) NAME=[Model] COUNT=(22) TYPE=[ASCII] VALUE=[Canon EOS 5D Mark III]
//   IFD=[IfdIdentity<PARENT-NAME=[] NAME=[IFD]>] ID=(0x0112) NAME=[Orientation] COUNT=(1) TYPE=[SHORT] VALUE=[1]
//   IFD=[IfdIdentity<PARENT-NAME=[] NAME=[IFD]>] ID=(0x011a) NAME=[XResolution] COUNT=(1) TYPE=[RATIONAL] VALUE=[72/1]
//   ...
package main

import (
	"flag"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cxcheng/exifutil"

	"gopkg.in/yaml.v2"
)

type Config struct {
	ExitOnFirstError bool     `yaml:"ExitOnFirstError"`
	FileExts         []string `yaml:"FileExts"`
	Tags             []string `yaml:"Tags"`
	LogPath          string   `yaml:"LogPath"`
	Tz               string   `yaml:"Timezone"`
	Verbose          bool     `yaml:"Verbose"`
}

func main() {
	var configF *os.File
	var logF *os.File
	var err error

	defer func() {
		if state := recover(); state != nil {
			log.Fatal("Exiting because of error...")
		}
	}()

	// Read command line arguments
	var (
		colsArg         = ""
		configArg       = ""
		outputArg       = ""
		filterArg       = ""
		orderArg        = ""
		orderReverseArg = false
		tagsArg         = ""
	)

	flag.StringVar(&configArg, "conf", "exif.yml", "Path of optional config YAML")
	flag.StringVar(&colsArg, "cols", "Sys/Name,Make,Model,DateTimeOriginal", "Columns to output")
	flag.StringVar(&filterArg, "filter", "", "Expression to filter")
	flag.StringVar(&orderArg, "order", "", "Sort order of output")
	flag.BoolVar(&orderReverseArg, "reverse", false, "Reverse order of sort output")
	flag.StringVar(&tagsArg, "tags", "", "Tags to extract and show")
	flag.StringVar(&outputArg, "out", "", "Output type: csv, json, keys")
	flag.Parse()

	var config Config
	var loc *time.Location

	// Read from config file first if it exists
	if configF, err = os.Open(configArg); err == nil {
		decoder := yaml.NewDecoder(configF)
		if err = decoder.Decode(&config); err != nil {
			log.Panicf("Error parsing config [%s], exiting...", configArg)
		}
		defer configF.Close()
	}

	// Setup log path
	if config.LogPath != "" {
		if logF, err = os.OpenFile(config.LogPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666); err == nil {
			log.SetOutput(logF)
		} else {
			log.SetOutput(os.Stderr)
		}
		defer logF.Close()
	}

	// Check and adjust configs
	if len(config.FileExts) == 0 {
		config.FileExts = []string{"jpg", "jpeg", "JPG", "JPEG"}
	}
	if tagsArg != "" {
		config.Tags = strings.Split(tagsArg, ",")
	}

	// Log status
	if config.Verbose {
		log.Println("Configuration")
		encoder := yaml.NewEncoder(logF)
		encoder.Encode(config)
	}

	// Set timezone if specified, otherwise, use local time zone
	if config.Tz != "" {
		if loc, err = time.LoadLocation(config.Tz); err != nil {
			log.Fatalf("Unable to load timezone [%s]", config.Tz)
			os.Exit(1)
		}
	} else {
		// otherwise use local location
		loc = time.Now().Local().Location()
	}

	// Setup command and output
	command := exifutil.Command{
		OutType:       outputArg,
		Cols:          strings.Split(colsArg, ","),
		Filter:        filterArg,
		OrderBy:       orderArg,
		OrderReversed: orderReverseArg,
		TagsToLoad:    strings.Split(tagsArg, ","),
		Tz:            loc,
	}
	out := exifutil.Output{}
	out.Init(&command)
	out.OutHeading()

	// Walkthrough arguments
	for _, arg := range flag.Args() {
		err := filepath.Walk(arg,
			func(path string, f os.FileInfo, err error) error {
				// Filter out file based on extension
				matchedExt := false
				if len(config.FileExts) > 0 {
					for _, ext := range config.FileExts {
						if filepath.Ext(path) == "."+ext {
							matchedExt = true
						}
					}
				} else {
					matchedExt = true
				}
				if matchedExt {
					if config.Verbose {
						log.Printf("Processing [%s]", path)
					}
					err := exifutil.Process(path, f, err, &command, &out)
					if config.ExitOnFirstError {
						return err
					} else {
						return nil
					}
				} else {
					return nil
				}
			})
		if err != nil {
			break
		}
	}

	// Print output
	out.OutBuffered()

	// Finally, done
	os.Exit(0)
}
