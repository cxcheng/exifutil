package exiftool

import (
	"context"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type outRecord struct {
	path  string
	cols  []string
	colVs []interface{}
}

type ExifDB struct {
	config *Config
	in     PipelineChan
	out    PipelineChan

	client     *mongo.Client
	mongoCtx   context.Context
	collection *mongo.Collection
}

func (ctx *ExifDB) AddArgs() {
}

func (ctx *ExifDB) Init(config *Config) error {
	var err error

	ctx.config = config
	ctx.out = make(PipelineChan)

	// Setup config
	dbName := config.DB.Name
	if dbName == "" {
		dbName = "exif_data"
		ctx.config.DB.Name = dbName
	}

	// Setup client
	var uri string
	if config.DB.URI == "" {
		uri = "mongodb://localhost:27017"
		ctx.config.DB.URI = uri
	} else {
		uri = config.DB.URI
	}
	log.Printf("Connecting to MongoDB [%v/%v]", uri, dbName)

	ctx.mongoCtx, _ = context.WithTimeout(context.Background(), 10*time.Second)
	if ctx.client, err = mongo.Connect(ctx.mongoCtx, options.Client().ApplyURI(uri)); err == nil {
		ctx.collection = ctx.client.Database(dbName).Collection("metadata")
		// Recreate collection from scratch
		if config.DB.DropFirst {
			log.Printf("Dropping collection metadata first")
			if err = ctx.collection.Drop(ctx.mongoCtx); err != nil {
				return err
			}
			// Recreate
			ctx.collection = ctx.client.Database(dbName).Collection("metadata")
		}
	}

	return err
}

func (ctx *ExifDB) SetInput(in PipelineChan) {
	ctx.in = in
}

func (ctx *ExifDB) GetOutput() PipelineChan {
	return ctx.out
}

func (ctx *ExifDB) Run(callOnExit func(time.Time, string)) {
	defer callOnExit(time.Now(), "Store Metadata")

	for {
		exifData := <-ctx.in
		if exifData == nil {
			// No more inputs, exit
			break
		}

		// Forward to next stage before DB work
		ctx.out <- exifData

		var err error

		// Insert database record
		var bdoc interface{}
		if err = bson.UnmarshalExtJSON([]byte(exifData.Json()), false, bdoc); err != nil {
			log.Printf("[%s] parse error: %s", exifData.Get("Sys/Path"), err)
			println(exifData.Json())
			// skip to next
			continue
		}
		var rs *mongo.InsertOneResult
		if rs, err = ctx.collection.InsertOne(ctx.mongoCtx, &bdoc); err != nil {
			log.Printf("[%s] insert error: %s", exifData.Get("Sys/Path"), err)
			continue
		}
		log.Printf("[%s] inserted record %v", rs.InsertedID)
	}
	// Signal exit
	ctx.out <- nil
}
