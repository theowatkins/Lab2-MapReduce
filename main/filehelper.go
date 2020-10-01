package main

import (
	"io/ioutil"
	"log"
	"os"
	"strings"
)

func aggregateFileContents(filePaths []string) string {
	aggregateContent := ""
	for _, filePath := range filePaths {
		content := readFileContents(filePath)
		aggregateContent += string(content) + " " //separates prev from next file
	}
	return strings.TrimSpace(aggregateContent)
}

func readFileContents(filePath string) string {
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("cannot open %v", filePath)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filePath)
	}
	file.Close()
	return string(content)
}

func splitStringIntoChunks(content string, chunksRequested int) []string {
	approximateNumberOfWords := strings.Count(" ", content) + 1 // one space = two words
	numChunks := approximateNumberOfWords
	if chunksRequested < approximateNumberOfWords {
		numChunks = chunksRequested
	}
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
