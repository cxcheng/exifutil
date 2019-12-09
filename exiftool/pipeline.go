package exiftool

import (
	"flag"
	"log"
	"os"
	"runtime"
	"sync"
	"time"

	"gopkg.in/yaml.v2"
)

type Config struct {
	ExitOnError bool   `yaml:"exit_on_error"`
	LogPath     string `yaml:"log_path"`
	Verbose     bool   `yaml:"verbose"`

	Input struct {
		FileExts   []string `yaml:"file_exts"`
		TagsToLoad []string `yaml:"tags_to_load"`
		Tz         string   `yaml:"timezone"`
		Trim       bool     `yaml:"trim"`
	} `yaml:"input"`
	DB struct {
		Name string `yaml:"name"`
		URI  string `yaml:"uri"`
	} `yaml:"database"`
	Output struct {
		Path string `yaml:"path"`
	} `yaml:"output"`
}

type Pipeline struct {
	conf   *Config
	input  *InputContext
	db     *DBContext
	output *OutputContext
}

func logElapsedTime(start time.Time, label string) {
	pc := make([]uintptr, 10) // at least 1 entry needed
	runtime.Callers(2, pc)
	f := runtime.FuncForPC(pc[0])
	//file, line := f.FileLine(pc[0])
	log.Printf("**** [%s:%s] elapsed time: %s, %d goroutines", label, f.Name(), time.Since(start), runtime.NumGoroutine())
}

func MakePipeline(confPath string) (*Pipeline, error) {
	// Read from config file first if it exists
	conf := new(Config)
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
			log.SetOutput(logF)
		} else {
			log.SetOutput(os.Stderr)
		}
	}

	// Setup pipeline components
	var err error
	pipeline := new(Pipeline)
	pipeline.conf = conf
	if pipeline.input, err = MakeInput(conf, logElapsedTime); err != nil {
		log.Printf("Error initiializing input: %s", err)
		return nil, err
	}
	if pipeline.db, err = MakeDB(conf, logElapsedTime, pipeline.input.Out); err != nil {
		log.Printf("Error initiializing database: %s", err)
		return nil, err
	}
	if pipeline.output, err = MakeOutput(conf, logElapsedTime, pipeline.db.Out); err != nil {
		log.Printf("Error initiializing output: %s", err)
		return nil, err
	}

	// Setup verbose param
	if !conf.Verbose {
		flag.BoolVar(&conf.Verbose, "verbose", false, "Verbose output, overrides config")
	}
	if conf.Verbose {
		log.Printf("Number of CPUs: %d", runtime.NumCPU())
	}
	return pipeline, nil
}

func (p *Pipeline) Run() {
	wg := sync.WaitGroup{}
	wg.Add(3)
	go p.input.ReadInputs(&wg)
	go p.db.StoreMetadata(&wg)
	go p.output.Output(&wg)
	wg.Wait()

}
