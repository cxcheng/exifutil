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
	"gopkg.in/yaml.v3"
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
	Throttle struct {
		MaxCPUs int `yaml:"max_cpus"`
		Rate    int `yaml:"rate"`
	} `yaml:"throttle"`
}

func MakeConfig(configPath string) (*Config, error) {
	var config *Config
	var err error

	config = new(Config)
	var f *os.File
	if f, err = os.Open(configPath); err == nil {
		decoder := yaml.NewDecoder(f)
		if err = decoder.Decode(config); err != nil {
			log.Printf("Error reading config [%s]: %s", configPath, err)
		}
		defer f.Close() // close immediately after exiting this
	}
	return config, err
}

type PipelineChan chan *exifutil.ExifData

type PipelineComponent interface {
	Init(config *Config) error
	Run() error
	SetInput(in PipelineChan)
	SetOutput(out PipelineChan)
}

type Pipeline struct {
	name      string
	verbose   bool
	component PipelineComponent
	next      *Pipeline
}

var PipelineComponentRegistry = map[string]reflect.Type{
	"input":    reflect.TypeOf(ExifInput{}),
	"output":   reflect.TypeOf(ExifOutput{}),
	"database": reflect.TypeOf(ExifDB{}),
}

func MakePipelineComponent(name string) (PipelineComponent, error) {
	if componentType, found := PipelineComponentRegistry[name]; found {
		if component, ok := reflect.New(componentType).Interface().(PipelineComponent); !ok {
			return nil, errors.New(fmt.Sprintf("Unable to cast [%s:%s] to PipelineComponent", name, componentType))
		} else {
			return component, nil
		}
	} else {
		return nil, errors.New(fmt.Sprintf("Unknown component %s", name))
	}
}

func MakePipeline(config *Config) (*Pipeline, error) {
	var pipeline *Pipeline = new(Pipeline)
	var err error

	// Build pipeline from config
	lastStage := pipeline
	for _, componentName := range config.Pipeline {
		var component PipelineComponent
		if component, err = MakePipelineComponent(componentName); component == nil {
			return nil, err
		}
		lastStage = lastStage.Add(componentName, component)
		if lastStage == nil {
			return nil, errors.New(fmt.Sprintf("Error adding component [%s]", componentName))
		}
	}

	// Initialize and parse command-line arguments
	err = pipeline.Init(config)
	return pipeline, err
}

func (p *Pipeline) Add(name string, component PipelineComponent) *Pipeline {
	// If first one, return itself after init
	if p.component == nil {
		p.name = name
		p.component = component
		return p
	}

	// Add() can only be called once
	if p.next != nil {
		log.Printf("[%s] Add() can only be called once", p.name)
		return nil
	}

	// Add to next stage, and setup input/output
	out := make(PipelineChan)
	p.component.SetOutput(out)
	component.SetInput(out)
	p.next = &Pipeline{name: name, component: component, next: nil}
	return p.next
}

func (p *Pipeline) Init(config *Config) error {
	// Initialize all the pipeline components
	for stage := p; stage != nil; { //stage = stage.next {
		if stage.component == nil {
			return fmt.Errorf("[%s] component not set up", stage.name)
		}
		if err := stage.component.Init(config); err != nil {
			// Exit on first error
			return fmt.Errorf("[%s] init error: %s", stage.name, err)
		}
		log.Printf("[%s] initialized", stage.name)
		stage = stage.next
		if stage != nil {
		}
	}
	return nil
}

func (p *Pipeline) Run() error {
	/*
		if p.config.Verbose {
			log.Printf("Number of CPUs: %d", runtime.NumCPU())
		}
	*/

	// Run pipeline components as goroutines
	wg := sync.WaitGroup{}

	var rtnErr error
	for stage := p; stage != nil; stage = stage.next {
		wg.Add(1)
		go func(name string, component PipelineComponent) {
			start := time.Now()
			if err := component.Run(); err != nil {
				log.Printf("[%s] error: %s", err)
				rtnErr = err
			}
			wg.Done()
			log.Printf("**** [%s] elapsed time: %s, %d goroutines", name, time.Since(start), runtime.NumGoroutine())
		}(stage.name, stage.component)
	}
	wg.Wait() // wait for all goroutines to exit

	return rtnErr
}
