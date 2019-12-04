package exifcommand

import (
	"flag"
)

type InfoCmd struct {
	conf     *Config
	showTags bool
}

func (cmd *InfoCmd) Init(conf *Config) error {
	cmd.conf = conf

	// Process command-line arguments
	flag.BoolVar(&cmd.showTags, "tags", true, "Show all supported tags")
	flag.Parse()

	return nil
}

func (cmd *InfoCmd) Run() {
}
