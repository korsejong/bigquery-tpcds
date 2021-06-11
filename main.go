package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"korsejong.github.com/bigquery/pkg/bigquery"
	"korsejong.github.com/bigquery/pkg/file"
)

func main() {
	ctx := context.Background()

	path := flag.String("path", "", "file dir path")
	output := flag.String("o", "./", "output dir path")
	projectID := flag.String("p", "", "gcp project id")
	datasetID := flag.String("d", "", "gcp dataset id")
	flag.Parse()

	fmt.Printf("path: %s\noutput: %s\nprojectID: %s\ndatasetID: %s\n", *path, *output, *projectID, *datasetID)

	queryFiles, err := file.List(*path)
	if err != nil {
		log.Panic(err)
	}

	for _, queryFile := range queryFiles {
		q, err := file.Read(queryFile)
		if err != nil {
			log.Println(err)
			continue
		}

		fileName := strings.Split(filepath.Base(queryFile), ".")[0]
		resFile, err := os.Create(filepath.Join(*output, fmt.Sprintf("res_%s", fileName)))
		logFile, err := os.Create(filepath.Join(*output, fmt.Sprintf("log_%s", fileName)))
		if err != nil {
			log.Println(err)
			continue
		}

		if err := bigquery.QueryDisableCache(ctx, *projectID, *datasetID, q, resFile, logFile); err != nil {
			log.Println(err)
		}
		resFile.Close()
		logFile.Close()
	}
}
