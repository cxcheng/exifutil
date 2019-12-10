package exiftool

import (
	"errors"
	"fmt"
	"log"
	"runtime"
	"sync"
	"time"

	"github.com/cxcheng/exifutil"
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
		Name string `yaml:"name"`
		URI  string `yaml:"uri"`
	} `yaml:"database"`
	Output struct {
		Path string `yaml:"path"`
	} `yaml:"output"`
}

type PipelineChan chan *exifutil.ExifData
type PipelineComponent interface {
	Run(callOnExit func(start time.Time, label string))
	SetInput(in *PipelineChan)
	GetOutput() *PipelineChan
}

type PipelineComponentMaker func(config *Config) (*PipelineComponent, error)

type Pipeline struct {
	config     *Config
	components []*PipelineComponent
}

var pipeComponentLookup = map[string]PipelineComponentMaker{
	"input": MakeInput,
}

func MakePipeline(config *Config) (*Pipeline, error) {
	var p *Pipeline
	var err error

	p = new(Pipeline)
	p.config = config

	// Build pipeline
	var previousOut *PipelineChan = nil
	for _, pipelineCompSpec := range config.Pipeline {
		if pipelineCompMaker, found := pipeComponentLookup[pipelineCompSpec]; found {
			var component *PipelineComponent
			if component, err = pipelineCompMaker(config); err == nil {
				(*component).SetInput(previousOut)
				// Add to pipeline
				p.components = append(p.components, component)
				previousOut = (*component).GetOutput()
				log.Printf("Added pipeline component [%s]", pipelineCompSpec)
			} else {
				return nil, err
			}
		} else {
			return nil, errors.New(fmt.Sprintf("Unknown component %s", pipelineCompSpec))
		}
	}

	return p, err
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
