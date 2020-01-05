package exifutil

import (
	"log"

	"go.mongodb.org/mongo-driver/mongo/options"

	"go.mongodb.org/mongo-driver/bson"
)

type MetadataDBQuery struct {
	out PipelineChan

	db      MetadataDB
	outCols bson.D
}

func (c *MetadataDBQuery) Init(config *Config) error {
	if err := c.db.Init(config); err != nil {
		return err
	}

	// Specify list of attributes
	c.outCols = bson.D{}
	for _, colName := range config.Output.Cols {
		c.outCols = append(c.outCols, bson.E{colName, 1})
	}

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
	/*
		for _, arg := range flag.Args()[1:] {
		}
	*/

	// Make query
	findOpt := options.FindOptions{}
	findOpt.SetProjection(c.outCols)
	cur, err := c.db.collection.Find(c.db.ctx, bson.D{}, &findOpt)
	if err != nil {
		return err
	}

	// Process output
	defer func() {
		// Send finish signal
		c.out <- nil
		// Close cursor when done
		cur.Close(c.db.ctx)
	}()
	outMsg := PipelineObj{}
	for cur.Next(c.db.ctx) {
		var doc map[string]interface{}

		// Decode DB cursor to map
		if err := cur.Decode(&doc); err != nil {
			return err
		}
		// Pass to next round
		outMsg.data = []Metadata{Metadata{V: doc}}
		c.out <- &outMsg
	}

	if err := cur.Err(); err != nil {
		return err
	}

	return nil
}
