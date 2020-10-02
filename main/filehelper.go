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

/* Splits given content into the requested number of chunks split on
 * given delimiter. Delimiter is not included in chunks.
 * Note, if chunksRequested < # of chunk delimiters the latter is used
 * as the number of chunks
 */
func splitStringIntoChunks(
	content string,
	chunksRequested int,
	chunkDelimiter string) []string {
	numberOfChunkDelimiters := strings.Count(content, chunkDelimiter) + 1 // one space = two chunks
	numChunks := numberOfChunkDelimiters
	if chunksRequested < numberOfChunkDelimiters {
		numChunks = chunksRequested
	}

	delimitersPerChunk := (numberOfChunkDelimiters / numChunks) - 1
	chunks := make([]string, numChunks)
	contentLength := len(content)

	startIndex := 0
	endIndex := 0
	chunkIndex := 0
	for endIndex < contentLength && chunkIndex < numChunks {
		//1. Determine end index to end on chunk delimiter
		endIndex = findEndOfChunk(content, chunkDelimiter, delimitersPerChunk, endIndex)

		//2. Create Chunk
		chunks[chunkIndex] = content[startIndex:endIndex]
		chunkIndex++
		endIndex++
		startIndex = endIndex

		//3. check for unfinished content
		if endIndex < contentLength && chunkIndex >= numChunks {
			lastChunkIndex := numChunks - 1
			remainingContent := content[endIndex-1 : contentLength]
			chunks[lastChunkIndex] = chunks[lastChunkIndex] + remainingContent
		}

	}
	return chunks
}

/* Returns the index marking the where the chunk should end
 * given that a number of delimiters must be seen in the chunk.
 */
func findEndOfChunk(
	content string,
	chunkDelimiter string,
	delimitersPerChunk int,
	startIndex int,
) int {
	contentLength := len(content)
	delimitersInChunk := 0
	for startIndex < contentLength {
		if string(content[startIndex]) == chunkDelimiter {
			delimitersInChunk++
		}

		if delimitersInChunk <= delimitersPerChunk {
			startIndex++
		} else {
			break
		}
	}
	return startIndex
}