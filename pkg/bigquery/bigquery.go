package bigquery

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
)

// QueryDisableCache demonstrates issuing a bigquery and requesting that the bigquery cache is bypassed.
func QueryDisableCache(ctx context.Context, projectID string, datasetID string, query string, resWriter io.Writer, logWriter io.Writer) error {
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return fmt.Errorf("bigquery.NewClient: %v", err)
	}
	defer client.Close()

	q := client.Query(query)
	q.DefaultDatasetID = datasetID
	q.DisableQueryCache = true
	q.Location = "asia-northeast3"

	// Run the bigquery and print results when the bigquery job is completed.
	job, err := q.Run(ctx)
	if err != nil {
		return err
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return err
	}

	if err := status.Err(); err != nil {
		return err
	}
	it, err := job.Read(ctx)
	for {
		var row []bigquery.Value
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}
		fmt.Fprintln(resWriter, row)
	}

	// Parse status
	statusJson, err := json.MarshalIndent(status, "", "  ")
	if err != nil {
		return err
	}
	fmt.Fprintln(logWriter, string(statusJson))
	return nil
}
