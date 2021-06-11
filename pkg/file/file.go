package file

import (
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
)

func List(root string) ([]string, error) {
	var queryFiles []string
	if err := filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}
		if filepath.Ext(p) == ".sql" {
			queryFiles = append(queryFiles, p)
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return queryFiles, nil
}

func Read(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		log.Println(err)
		return "", err
	}

	defer f.Close()

	q, err := ioutil.ReadAll(f)
	if err != nil {
		return "", err
	}

	return string(q), nil
}
