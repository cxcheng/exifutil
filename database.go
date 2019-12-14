package exifutil

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"reflect"
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
		o := <-c.in
		if c.out != nil {
			// Forward to next stage
			c.out <- o
		}
		if o == nil || o.err != nil {
			// No more inputs, exit
			break
		}

		var err error
		for _, md := range o.data {
			path := md.Expr("FileName")

			// Check if there is existing record
			findRs := c.collection.FindOne(c.ctx, bson.M{"Key": md.Expr("Key")})
			var findRsV interface{}
			if err = findRs.Decode(&findRsV); err == nil {
				// Update existing record
				/*
					var rs *mongo.UpdateResult
					if rs, err = c.collection.UpdateOne(c.ctx, bson.M{"_id": findRsV["_id"]}, md.V); err != nil {
						log.Printf("[%s] insert error: %s", path, err)
						continue
					}
				*/
				s, err := json.Marshal(findRsV)
				print(err)
				log.Printf("[%s] updated record [%s] %s %s", path, reflect.TypeOf(findRsV), s)
			} else {
				// Insert database record
				var rs *mongo.InsertOneResult
				if rs, err = c.collection.InsertOne(c.ctx, md.V); err != nil {
					log.Printf("[%s] insert error: %s", path, err)
					continue
				}
				log.Printf("[%s] inserted record [%v]", path, rs.InsertedID)
			}

		}
	}
	return nil
}
