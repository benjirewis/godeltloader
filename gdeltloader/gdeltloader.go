package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"os"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
)

// Argument defaults
var (
	mongoURI     string = "mongodb://localhost:27017"
	masterFile   string = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"
	currBatchDir string = "data"
	batchSize    int64  = 5
	limit        int64  = 5
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
	if err := scanner.Err(); err != nil {
		return false, err
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
	err = unzip("currZip.zip", currBatchDir)
	if err != nil {
		return err
	}

	// Delete currZip.zip.
	return os.Remove("currZip.zip")
}

// processData parses a batch of local data in CSV to an array of documents.
func processData() ([]bsoncore.Document, error) {
	var documents []bsoncore.Document

	// Parse each file in current batch directory and add to documents.
	files, err := ioutil.ReadDir(currBatchDir)
	if err != nil {
		return nil, err
	}
	for _, fp := range files {
		filename := fmt.Sprintf("%s/%s", currBatchDir, fp.Name())
		document, err := processDocument(filename)
		if err != nil {
			return nil, err
		}
		documents = append(documents, document...)
	}

	// Remove files from current batch directory.
	if err := removeContents(currBatchDir); err != nil {
		return nil, err
	}

	return documents, nil
}

// processDocument parses a single GDELT CSV file to a bsoncore Document.
func processDocument(fp string) ([]bsoncore.Document, error) {
	var docs []bsoncore.Document

	file, err := os.Open(fp)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := bytes.Split(scanner.Bytes(), []byte{'\t'})
		var doc bsoncore.Document

		// EventID, date and URL attributes processing.
		if len(fields[0]) > 1 { // Guard all endian reads to avoid panics.
			doc = bsoncore.AppendInt32Element(doc, "GlobalEventID", int32(binary.BigEndian.Uint32(fields[0])))
		}
		doc = bsoncore.AppendDateTimeElement(doc, "Date", createDateTimeElement(string(fields[59]))) // FIX THISSSSS
		doc = bsoncore.AppendStringElement(doc, "SourceURL", string(fields[60]))

		// Actor attributes. Contains optionals
		doc = bsoncore.AppendStringElement(doc, "Actor1Code", string(fields[5]))
		doc = bsoncore.AppendStringElement(doc, "Actor1Name", string(fields[6]))
		doc = bsoncore.AppendStringElement(doc, "Actor1CountryCode", string(fields[7]))
		doc = bsoncore.AppendStringElement(doc, "Actor1KnownGroupCode", string(fields[8]))
		doc = bsoncore.AppendStringElement(doc, "Actor1EthnicCode", string(fields[9]))
		doc = bsoncore.AppendStringElement(doc, "Actor1Religion1Code", string(fields[10]))
		doc = bsoncore.AppendStringElement(doc, "Actor1Religion2Code", string(fields[11]))
		doc = bsoncore.AppendStringElement(doc, "Actor1Type1Code", string(fields[12]))
		doc = bsoncore.AppendStringElement(doc, "Actor1Type2Code", string(fields[13]))
		doc = bsoncore.AppendStringElement(doc, "Actor1Type3Code", string(fields[14]))

		doc = bsoncore.AppendStringElement(doc, "Actor2Code", string(fields[15]))
		doc = bsoncore.AppendStringElement(doc, "Actor2Name", string(fields[16]))
		doc = bsoncore.AppendStringElement(doc, "Actor2CountryCode", string(fields[17]))
		doc = bsoncore.AppendStringElement(doc, "Actor2KnownGroupCode", string(fields[18]))
		doc = bsoncore.AppendStringElement(doc, "Actor2EthnicCode", string(fields[19]))
		doc = bsoncore.AppendStringElement(doc, "Actor2Religion1Code", string(fields[20]))
		doc = bsoncore.AppendStringElement(doc, "Actor2Religion2Code", string(fields[21]))
		doc = bsoncore.AppendStringElement(doc, "Actor2Type1Code", string(fields[22]))
		doc = bsoncore.AppendStringElement(doc, "Actor2Type2Code", string(fields[23]))
		doc = bsoncore.AppendStringElement(doc, "Actor2Type3Code", string(fields[24]))

		// Event action attributes.
		if len(fields[25]) > 1 {
			doc = bsoncore.AppendBooleanElement(doc, "IsRootEvent", int32(binary.BigEndian.Uint32(fields[25])) != 0)
		}
		doc = bsoncore.AppendStringElement(doc, "EventCode", string(fields[26]))
		doc = bsoncore.AppendStringElement(doc, "EventBaseCode", string(fields[27]))
		doc = bsoncore.AppendStringElement(doc, "EventRootCode", string(fields[28]))
		if len(fields[29]) > 1 {
			doc = bsoncore.AppendInt32Element(doc, "QuadClass", int32(binary.BigEndian.Uint32(fields[29])))
		}
		if len(fields[30]) > 1 {
			doc = bsoncore.AppendDoubleElement(doc, "GoldsteinScale", math.Float64frombits(binary.BigEndian.Uint64(fields[30])))
		}
		if len(fields[31]) > 1 {
			doc = bsoncore.AppendInt32Element(doc, "NumMentions", int32(binary.BigEndian.Uint32(fields[31])))
		}
		if len(fields[32]) > 1 {
			doc = bsoncore.AppendInt32Element(doc, "NumSources", int32(binary.BigEndian.Uint32(fields[32])))
		}
		if len(fields[33]) > 1 {
			doc = bsoncore.AppendInt32Element(doc, "NumArticles", int32(binary.BigEndian.Uint32(fields[33])))
		}
		if len(fields[34]) > 1 {
			doc = bsoncore.AppendInt32Element(doc, "AvgTone", int32(binary.BigEndian.Uint32(fields[34])))
		}

		// Event geography attributes.
		if len(fields[35]) > 1 {
			doc = bsoncore.AppendInt32Element(doc, "Actor1Geo_Type", int32(binary.BigEndian.Uint32(fields[35])))
		}
		doc = bsoncore.AppendStringElement(doc, "Actor1Geo_Fullname", string(fields[36]))
		doc = bsoncore.AppendStringElement(doc, "Actor1Geo_CountryCode", string(fields[37]))
		doc = bsoncore.AppendStringElement(doc, "Actor1Geo_ADM1Code", string(fields[38]))
		doc = bsoncore.AppendStringElement(doc, "Actor1Geo_ADM2Code", string(fields[39]))
		doc = bsoncore.AppendDocumentElement(doc, "Actor1Geo", createGeoElement(fields[40], fields[41])) // FIX THISSSS
		doc = bsoncore.AppendStringElement(doc, "Actor1Geo_FeatureID", string(fields[42]))

		if len(fields[43]) > 1 {
			doc = bsoncore.AppendInt32Element(doc, "Actor2Geo_Type", int32(binary.BigEndian.Uint32(fields[43])))
		}
		doc = bsoncore.AppendStringElement(doc, "Actor2Geo_Fullname", string(fields[44]))
		doc = bsoncore.AppendStringElement(doc, "Actor2Geo_CountryCode", string(fields[45]))
		doc = bsoncore.AppendStringElement(doc, "Actor2Geo_ADM1Code", string(fields[46]))
		doc = bsoncore.AppendStringElement(doc, "Actor2Geo_ADM2Code", string(fields[47]))
		doc = bsoncore.AppendDocumentElement(doc, "Actor2Geo", createGeoElement(fields[48], fields[49])) // FIX THISSSS
		doc = bsoncore.AppendStringElement(doc, "Actor2Geo_FeatureID", string(fields[50]))

		if len(fields[43]) > 1 {
			doc = bsoncore.AppendInt32Element(doc, "ActionGeo_Type", int32(binary.BigEndian.Uint32(fields[51])))
		}
		doc = bsoncore.AppendStringElement(doc, "ActionGeo_Fullname", string(fields[52]))
		doc = bsoncore.AppendStringElement(doc, "ActionGeo_CountryCode", string(fields[53]))
		doc = bsoncore.AppendStringElement(doc, "ActionGeo_ADM1Code", string(fields[54]))
		doc = bsoncore.AppendStringElement(doc, "ActionGeo_ADM2Code", string(fields[55]))
		doc = bsoncore.AppendDocumentElement(doc, "ActionGeo", createGeoElement(fields[56], fields[57])) // FIX THISSSS
		doc = bsoncore.AppendStringElement(doc, "ActionGeo_FeatureID", string(fields[58]))

		// Append doc to docs.
		docs = append(docs, doc)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return docs, nil
}

// uploadData inserts an array of bsoncore Documents into a collection as fast
// as possible.
func uploadData(docs []bsoncore.Document) error {
	return nil
}

// runUploader downloads, processes and uploads GDELT data to set mongoURI.
func runUploader() error {
	var err error
	for {
		cont, err := downloadData()
		if err != nil {
			break
		}

		docs, err := processData()
		if err != nil {
			break
		}

		err = uploadData(docs)
		if !cont {
			break
		}
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
