package exiftool

import (
	"errors"
	"fmt"
	"log"
	"os"
	"reflect"
	"runtime"
	"sync"
	"time"

	"github.com/cxcheng/exifutil"
	"gopkg.in/yaml.v2"
)

type Config struct {
	ExitOnError bool   `yaml:"exit_on_error"`
	LogPath     string `yaml:"log_path"`
	Verbose     bool   `yaml:"verbose"`

	Pipeline []string `yaml:"pipeline"`
	Input    struct {
		FileExts   []string `yaml:"file_exts"`
		TagsToLoad []string `yaml:"tags_to_load"`
		Tz         string   `yaml:"timezone"`
		Trim       bool     `yaml:"trim"`
	} `yaml:"input"`
	DB struct {
		Name      string `yaml:"name"`
		URI       string `yaml:"uri"`
		DropFirst bool   `yaml:"drop_first"`
	} `yaml:"database"`
	Output struct {
		Path string `yaml:"path"`
	} `yaml:"output"`
}

type PipelineChan chan *exifutil.ExifData
type PipelineComponent interface {
	AddArgs()
	Init(config *Config) error
	Run(callOnExit func(start time.Time, label string))
	SetInput(in PipelineChan)
	GetOutput() PipelineChan
}

type Pipeline struct {
	config     *Config
	components []*PipelineComponent

	configPath string
}

var pipeComponentLookup = map[string]reflect.Type{
	"input":    reflect.TypeOf(ExifInput{}),
	"database": reflect.TypeOf(ExifDB{}),
	"output":   reflect.TypeOf(ExifOutput{}),
}

func MakeConfig(configPath string) (*Config, error) {
	var config *Config
	var err error
	var f *os.File

	config = new(Config)
	if f, err = os.Open(configPath); err == nil {
		decoder := yaml.NewDecoder(f)
		if err = decoder.Decode(config); err != nil {
			log.Printf("Error reading config [%s]: %s", configPath, err)
		}
		defer f.Close() // close immediately after exiting this
	}
	return config, err
}

func (p *Pipeline) AddArgs() {
	// no arguments to add
}

func (p *Pipeline) Init(config *Config) error {
	var err error

	p.config = config

	// Build pipeline
	var previousOut PipelineChan = nil
	for i, componentName := range config.Pipeline {
		if componentType, found := pipeComponentLookup[componentName]; found {
			var component PipelineComponent
			var ok bool
			if component, ok = reflect.New(componentType).Interface().(PipelineComponent); !ok {
				return errors.New(fmt.Sprintf("Unable to cast [%s:%s] to PipelineComponent", componentName, componentType))
			}
			component.AddArgs()
			if err = component.Init(config); err == nil {
				component.SetInput(previousOut)
				// Add to pipeline
				p.components = append(p.components, &component)
				previousOut = component.GetOutput()
				log.Printf("Added pipeline [%s]", componentName)
				// Check if pipeline component has correct output
				if i < (len(config.Pipeline)-1) && component.GetOutput() == nil {
					return errors.New(fmt.Sprintf("Non-last pipeline component %s has no output", componentName))
				}
			} else {
				return err
			}
		} else {
			return errors.New(fmt.Sprintf("Unknown component %s", componentName))
		}
	}

	return err
}

func (p *Pipeline) Run(callOnExit func(start time.Time, label string)) {
	if p.config.Verbose {
		log.Printf("Number of CPUs: %d", runtime.NumCPU())
	}

	// Run pipeline components as goroutines
	wg := sync.WaitGroup{}
	for _, component := range p.components {
		wg.Add(1)
		go (*component).Run(func(start time.Time, label string) {
			callOnExit(start, label)
			wg.Done() // signal we are done
		})
	}
	wg.Wait() // wait for all goroutines to exit
}
