package main

import (
	"io/ioutil"
	"log"
	"os"
	"strings"
)

func aggregateFileContents(fileNames []string) string{
	allContent := ""
	for _, filename := range fileNames {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		allContent += string(content) + " "
	}
	return strings.TrimSpace(allContent)
}

func splitStringIntoChunks(content string, numChunks int) []string {
	chunks := make([]string, numChunks)
	contentLength := len(content)
	shardSize := contentLength / numChunks

	for i := 0; i < numChunks; i++ {
		startIndex := shardSize * i
		endIndex := startIndex + shardSize
		chunks[i] = content[startIndex:endIndex]
	}
	return chunks
}
