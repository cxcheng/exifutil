package exifutil

import (
	"flag"
	"log"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MetadataDBQuery struct {
	out PipelineChan

	db   MetadataDB
	cols []string
}

func (c *MetadataDBQuery) Init(config *Config) error {
	if err := c.db.Init(config); err != nil {
		return err
	}

	c.cols = config.Output.Cols

	return nil
}

func (c *MetadataDBQuery) SetInput(in PipelineChan) {
	if in != nil {
		log.Fatalf("[DBQuery] Input not allowed")
	}
}

func (c *MetadataDBQuery) SetOutput(out PipelineChan) {
	c.out = out
}

func (c *MetadataDBQuery) Run() error {
	// Setup projection

	for {
		findOpt := options.FindOptions{}
		for _, arg := range flag.Args()[1:] {
			path := md.Expr("FileName")

			// Try replace any existing record first, if none, then insert
			// We do that by using unique keyls
			id := md.ConstructKey()
			md.V["_id"] = id
			replaceResult, err := c.collection.ReplaceOne(c.ctx, bson.M{"_id": id}, md.V)
			if err != nil {
				log.Printf("[Database] [%s] replace error: %s", path, err)
				continue
			}
			if replaceResult.MatchedCount == 0 {
				// Cannot replace, we will try to insert
				insertResult, err := c.collection.InsertOne(c.ctx, md.V)
				if err != nil {
					log.Printf("[Database] [%s] insert: %s", path, err)
					continue
				}
				log.Printf("[Database] [%s] inserted %v", path, insertResult.InsertedID)
			} else {
				log.Printf("[Database] [%s] replaced [%s]", path, id)
			}
		}
	}
	return nil
}
