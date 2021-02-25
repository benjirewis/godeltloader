package main

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

// Argument defaults
var (
	mongoURI     string = "mongodb://localhost:27017"
	masterFile   string = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"
	currBatchDir string = "data"
	batchSize    int64  = 5
	limit        int64  = 5
	mapGeo       bool   = false
)

// currPos keeps track of current position in the provided ziplist
var currPos int64

// downloadData downloads the next batch of data from GDELT and (over)writes
// it to a local file; returns false when no more data is available
// and true otherwise.
func downloadData() (bool, error) {
	// get ziplist file
	ziplistHTTP, err := http.Get(masterFile)
	if err != nil {
		return false, err
	}

	// download only "export" files (ignore mentions and gkg)
	scanner := bufio.NewScanner(ziplistHTTP.Body)
	defer ziplistHTTP.Body.Close()
	for scanner.Scan() {
		err := downloadFile(scanner.Text())
		if err != nil {
			return false, err
		}

		// skip mentions and gkg
		_ = scanner.Scan()
		_ = scanner.Scan()

		if currPos++; currPos >= batchSize {
			break
		}
	}

	if currPos >= limit {
		return false, nil
	}
	return true, nil
}

// downloadFile downloads and unzips a single zip "line" from the ziplist into
// the current batch directory (currBatchDir)
func downloadFile(zipLine string) error {
	// Download zip file to currZip.zip.
	url := strings.Fields(zipLine)[2]
	csvZipHTTP, err := http.Get(url)
	if err != nil {
		return err
	}
	defer csvZipHTTP.Body.Close()

	out, err := os.Create("currZip.zip")
	if err != nil {
		return err
	}
	defer out.Close()
	io.Copy(out, csvZipHTTP.Body)

	// Unzip currZip.zip to current batch directory.
	err = Unzip("currZip.zip", currBatchDir)
	if err != nil {
		return err
	}

	// Delete currZip.zip.
	return os.Remove("currZip.zip")
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

// runUploader downloads, processes and uploads GDELT data to set mongoURI.
func runUploader() error {
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
	return err
}

func main() {
	start := time.Now()
	if err := runUploader(); err != nil {
		fmt.Printf("Error during execution: %v\n", err)
	}
	end := time.Now()
	fmt.Printf("Time to execute: %v\n", end.Sub(start))
}
