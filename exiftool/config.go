package exiftool

import (
	"log"
	"os"

	"gopkg.in/yaml.v2"
)

type Config struct {
	ExitOnError bool     `yaml:"input.exit_on_error"`
	FileExts    []string `yaml:"input.file_exts"`
	LogPath     string   `yaml:"log_path"`
	TagsToLoad  []string `yaml:"input.tags_to_load"`
	Tz          string   `yaml:"input.timezone"`
	Trim        bool     `yaml:"input.trim"`
	Verbose     bool     `yaml:"verbose"`
}

func MakeConfig(confPath string) *Config {
	conf := new(Config)

	// Read from config file first if it exists
	if len(confPath) > 0 {
		if f, err := os.Open(confPath); err == nil {
			decoder := yaml.NewDecoder(f)
			if err = decoder.Decode(conf); err != nil {
				log.Panicf("Error parsing config [%s], exiting...", confPath)
			}
			defer f.Close() // close immediately after exiting this
		}
	}

	// Setup log path
	var logF *os.File
	if conf.LogPath != "" {
		var err error
		if logF, err = os.Create(conf.LogPath); err == nil {
			//if logF, err = os.OpenFile(conf.LogPath, os.O_RDWR|os.O_CREATE, 0666); err == nil {
			log.SetOutput(logF)
		} else {
			log.SetOutput(os.Stderr)
		}
	}

	return conf
}
