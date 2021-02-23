package godeltloader

import (
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

var (
	mongoURI      string = "mongodb://localhost:27017"
	masterFile    string = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"
	currBatchFile string = "../batch.csv"
	batchSize     int64  = 365
	writeConcern  writeconcern.WriteConcern
	mapGeo        bool = false
)

// downloadData downloads the next batch of data from GDELT and (over)writes
// it to a local file; returns false when no more data is available
// and true otherwise.
func downloadData() (bool, error) {
	return true, nil
}

// processData parses a batch of local data in CSV to an array of
// Documents using the defined mdb document schema.
func processData() ([]interface{}, error) {
	return nil, nil
}

// uploadData inserts the data into a collection on the server as fast
// as possible.
func uploadData(docs []interface{}) error {
	return nil
}

func main() {
	start := time.Now()

	var err error
	for {
		cont, err := downloadData()
		if !cont || err != nil {
			break
		}

		docs, err := processData()
		if err != nil {
			break
		}

		err = uploadData(docs)
	}
	if err != nil {
		fmt.Printf("Error during execution: %v\n", err)
	}

	end := time.Now()
	fmt.Printf("Time to execute: %v\n", end.Sub(start))
}
