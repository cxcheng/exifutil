package exifutil

import (
	"errors"
	"log"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MetadataDBStore struct {
	in  PipelineChan
	out PipelineChan

	db MetadataDB
}

func (c *MetadataDBStore) Init(config *Config) error {
	return c.db.Init(config)
}

func (c *MetadataDBStore) SetInput(in PipelineChan) {
	c.in = in
}

func (c *MetadataDBStore) SetOutput(out PipelineChan) {
	c.out = out
}

func (c *MetadataDBStore) Run() error {
	if c.in == nil {
		return errors.New("[DBStore] No input defined")
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

		updateOpt := options.ReplaceOptions{}
		updateOpt.SetUpsert(true)
		for _, md := range o.data {
			path := md.Expr("FileName")

			// Adjust coordinates to use MongoDB GeoJSON
			if longitudeI, found := md.V["GPSLongitude"]; found {
				if latitudeI, found := md.V["GPSLatitude"]; found {
					if longitude, ok := longitudeI.(float64); ok {
						if latitude, ok := latitudeI.(float64); ok {
							md.V["Location"] = MetadataDBLocation{
								"Point",
								[]float64{longitude, latitude},
							}
						}
					}
				}
			}

			// Try replace any existing record first, if none, then insert
			// We do that by using unique keyls
			id := md.ConstructKey()
			md.V["_id"] = id
			replaceResult, err := c.db.collection.ReplaceOne(c.db.ctx, bson.M{"_id": id}, md.V)
			if err != nil {
				log.Printf("[DBStore] [%s] replace error: %s", path, err)
				continue
			}
			if replaceResult.MatchedCount == 0 {
				// Cannot replace, we will try to insert
				insertResult, err := c.db.collection.InsertOne(c.db.ctx, md.V)
				if err != nil {
					log.Printf("[DBStore] [%s] insert: %s", path, err)
					continue
				}
				log.Printf("[DBStore] [%s] inserted %v", path, insertResult.InsertedID)
			} else {
				log.Printf("[DBStore] [%s] replaced [%s]", path, id)
			}
		}
	}
	return nil
}
