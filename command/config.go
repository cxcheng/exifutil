package exifcommand

import (
	"flag"
	"log"
	"os"

	"gopkg.in/yaml.v2"
)

type Config struct {
	ExitOnFirstError bool     `yaml:"ExitOnFirstError"`
	FileExts         []string `yaml:"FileExts"`
	LogPath          string   `yaml:"LogPath"`
	Tags             []string `yaml:"Tags"`
	Tz               string   `yaml:"Timezone"`
	Trim             bool     `yaml:"Trim"`
	Verbose          bool     `yaml:"Verbose"`
}

func MakeConfig() *Config {
	conf := new(Config)

	// Read from config file first if it exists
	var confPath string
	flag.StringVar(&confPath, "conf", "exif.yml", "Path of optional config YAML")
	if f, err := os.Open(confPath); err == nil {
		decoder := yaml.NewDecoder(f)
		if err = decoder.Decode(conf); err != nil {
			log.Panicf("Error parsing config [%s], exiting...", confPath)
		}
		defer f.Close()
	}

	// Setup log path
	var logF *os.File
	if conf.LogPath != "" {
		var err error
		if logF, err = os.OpenFile(conf.LogPath, os.O_RDWR|os.O_CREATE, 0666); err == nil {
			log.SetOutput(logF)
		} else {
			log.SetOutput(os.Stderr)
		}
	}

	// Check and adjust configs
	if len(conf.FileExts) == 0 {
		conf.FileExts = []string{"heic", "HEIC", "jpg", "jpeg", "JPG", "JPEG", "tif", "TIF", "tiff", "TIFF"}
	}
	return conf
}
