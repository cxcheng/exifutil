package exiftool

import (
	"context"
	"log"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/cxcheng/exifutil"
)

type outRecord struct {
	path  string
	cols  []string
	colVs []interface{}
}

type DBConfig struct {
	Name string `yaml:"name"`
	URI  string `yaml:"uri"`
}

type DBContext struct {
	Out chan *exifutil.ExifData

	conf       *Config
	in         chan *exifutil.ExifData
	callOnExit func(time.Time, string)
	verbose    bool

	client     *mongo.Client
	collection *mongo.Collection
}

func MakeDB(conf *Config, callOnExit func(time.Time, string), in chan *exifutil.ExifData) (*DBContext, error) {
	defer callOnExit(time.Now(), "Init DB")

	var ctx DBContext
	var err error

	ctx = DBContext{conf: conf, callOnExit: callOnExit, in: in, Out: make(chan *exifutil.ExifData)}

	// Process command-line arguments
	/*
		var cols, outPath, sortCol string
		flag.StringVar(&cols, "cols", "Sys/Name,Sys/Key,Make,Model,DateTimeOriginal", "Columns to output")
		flag.StringVar(&outPath, "out", "", "Output path")
		flag.StringVar(&cmd.outType, "type", "", "Output type: csv, json, keys")
		flag.BoolVar(&cmd.value, "value", false, "Output value instead of original text")
		flag.Parse()
	*/

	// Setup config
	dbName := ctx.conf.DB.Name
	if dbName == "" {
		dbName = "exif_data"
		ctx.conf.DB.Name = dbName
	}

	// Setup client
	var uri string
	if ctx.conf.DB.URI == "" {
		uri = "mongodb://localhost:27017"
		ctx.conf.DB.URI = uri
	} else {
		uri = ctx.conf.DB.URI
	}
	log.Printf("Connecting to MongoDB [%v/%v]", uri, dbName)

	mongoCtx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	if ctx.client, err = mongo.Connect(mongoCtx, options.Client().ApplyURI(uri)); err == nil {
		ctx.collection = ctx.client.Database(dbName).Collection("exif")
	}

	return &ctx, err
}

func (ctx *DBContext) StoreMetadata(wg *sync.WaitGroup) {
	defer ctx.callOnExit(time.Now(), "Store DB")

	for {
		exifData := <-ctx.in
		if exifData == nil {
			// No more inputs, exit
			ctx.Out <- nil
			break
		}
	}
	wg.Done()
}
