package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"golang.org/x/sync/errgroup"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
)

type Data struct {
	// Define the structure of your JSON data here
	ID    int    `json:"id"`
	Value string `json:"value"`
}

// getDataFromFile reads and unmarshals JSON data from a file
func getDataFromFile(filename string) (result *Data, err error) {
	file, err := os.Open(filename)
	if err != nil {
		log.Printf("Error opening file %s: %v", filename, err)
		return
	}
	defer func() {
		closeErr := file.Close()
		if closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}()

	bytes, err := io.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("reading error: %w", err)
	}

	var data Data
	err = json.Unmarshal(bytes, &data)
	if err != nil {
		return nil, fmt.Errorf("unmarshalling error: %w", err)
	}

	return &data, nil
}

func main() {
	// specify the directory containing JSON files - would be cool to use flag here
	dir := "./json_files"

	// create a channel to receive results
	results := make(chan *Data, 10)

	go func() {
		for {
			select {
			case r := <-results:
				log.Printf("data with id #%d is received: %s\n", r.ID, r.Value)
			}
		}
	}()

	group := errgroup.Group{}
	// walk through the directory and read JSON files concurrently
	err := filepath.Walk(dir, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && filepath.Ext(path) == ".json" {
			group.Go(func() error {
				data, err := getDataFromFile(path)
				if err == nil {
					results <- data
				}
				return err
			})
		}
		return nil
	})
	if err != nil {
		log.Fatalf("Error walking the path %v: %v", dir, err)
	}

	go func() {
		defer close(results)
		if err := group.Wait(); err != nil {
			log.Fatalf("Error waiting on error group: %v", err)
		}
	}()

	// Process results
	for r := range results {
		log.Printf("data with id #%d is received: %s\n", r.ID, r.Value)
	}
}
