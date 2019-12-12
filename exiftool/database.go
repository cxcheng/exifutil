package exiftool

import (
	"context"
	"errors"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type ExifDB struct {
	in  PipelineChan
	out PipelineChan

	client     *mongo.Client
	ctx        context.Context
	collection *mongo.Collection
}

func (c *ExifDB) Init(config *Config) error {
	var err error

	// Setup config
	dbName := config.DB.Name
	if dbName == "" {
		dbName = "exif_data"
	}

	// Setup client
	var uri string
	if config.DB.URI == "" {
		uri = "mongodb://localhost:27017"
	} else {
		uri = config.DB.URI
	}
	log.Printf("Connecting to MongoDB [%v/%v]", uri, dbName)

	c.ctx, _ = context.WithTimeout(context.Background(), 10*time.Second)
	println(uri)
	if c.client, err = mongo.Connect(c.ctx, options.Client().ApplyURI(uri)); err == nil {
		c.collection = c.client.Database(dbName).Collection("metadata")
		// Recreate collection from scratch
		if config.DB.DropFirst {
			log.Printf("Dropping collection metadata first")
			if err = c.collection.Drop(c.ctx); err != nil {
				return err
			}
			// Recreate
			c.collection = c.client.Database(dbName).Collection("metadata")
		}
	}
	return err
}

func (c *ExifDB) SetInput(in PipelineChan) {
	c.in = in
}

func (c *ExifDB) SetOutput(out PipelineChan) {
	c.out = out
}

func (c *ExifDB) Run() error {
	if c.in == nil {
		return errors.New("No input defined")
	}

	for {
		exifData := <-c.in
		if c.out != nil {
			// Forward to next stage
			c.out <- exifData
		}
		if exifData == nil {
			// No more inputs, exit
			break
		}

		var err error

		// Forward to next stage

		// Insert database record
		var bdoc interface{}
		if err = bson.UnmarshalExtJSON([]byte(exifData.Json()), false, bdoc); err != nil {
			log.Printf("[%s] parse error: %s", exifData.Get("Sys/Path"), err)
			println(exifData.Json())
			// skip to next
			continue
		}
		var rs *mongo.InsertOneResult
		if rs, err = c.collection.InsertOne(c.ctx, &bdoc); err != nil {
			log.Printf("[%s] insert error: %s", exifData.Get("Sys/Path"), err)
			continue
		}
		log.Printf("[%s] inserted record %v", rs.InsertedID)

	}
	return nil
}
