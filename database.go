package exifutil

import (
	"context"
	"errors"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MetadataDB struct {
	in  PipelineChan
	out PipelineChan

	client     *mongo.Client
	ctx        context.Context
	collection *mongo.Collection
}

func (c *MetadataDB) Init(config *Config) error {
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

func (c *MetadataDB) SetInput(in PipelineChan) {
	c.in = in
}

func (c *MetadataDB) SetOutput(out PipelineChan) {
	c.out = out
}

func (c *MetadataDB) Run() error {
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

		for _, md := range o.data {
			path := md.Expr("FileName")

			// Check if there is existing record
			id := md.ConstructKey()
			findRs := c.collection.FindOne(c.ctx, bson.M{"_id": id})
			var findRsV interface{}
			if err := findRs.Decode(&findRsV); err == nil {
				// Update existing record
				if _, err := c.collection.ReplaceOne(c.ctx, bson.M{"_id": id}, md.V); err != nil {
					log.Printf("[%s] replace error: %s", path, err)
					continue
				}
				log.Printf("[%s] updated record [%v]", path, id)
			} else {
				// Insert database record
				md.V["_id"] = id
				rs, err := c.collection.InsertOne(c.ctx, md.V)
				if err != nil {
					log.Printf("[%s] insert error: %s", path, err)
					continue
				}
				log.Printf("[%s] inserted record [%v] %s", path, rs.InsertedID, id)
			}

		}
	}
	return nil
}
