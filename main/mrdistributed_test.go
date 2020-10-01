package main

import (
	"testing"
)

func TestGetAggregateContentInFiles(t *testing.T) {
	var fileNames = []string{
		"../testResources/testFileOne.txt",
		"../testResources/testFileTwo.txt"}

	content := aggregateFileContents(fileNames)
	var expected = "hello world this is another test this is another test"
	assertStringsEqual(expected, content, t)
}

func TestRegionOfUniqueKeys(t *testing.T) {
	intermediatePairs := make([]KeyValue, 1)
	intermediatePairs[0] = KeyValue{"all", "1"}
	result := getRegionsForUniqueKeys(&intermediatePairs)

	assertIntegersEqual(1, len(result), t)
	assertIntegersEqual(0, result[0].start, t)
	assertIntegersEqual(1, result[0].end, t)
}

func TestRegionOfUniqueKeysMultipleSame(t *testing.T) {
	intermediatePairs := make([]KeyValue, 2)
	intermediatePairs[0] = KeyValue{"all", "1"}
	intermediatePairs[1] = KeyValue{"all", "1"}
	result := getRegionsForUniqueKeys(&intermediatePairs)

	assertIntegersEqual(1, len(result), t)
	assertIntegersEqual(0, result[0].start, t)
	assertIntegersEqual(2, result[0].end, t)
}

func TestRegionOfUniqueKeysMultipleDifferent(t *testing.T) {
	intermediatePairs := make([]KeyValue, 2)
	intermediatePairs[0] = KeyValue{"all", "1"}
	intermediatePairs[1] = KeyValue{"two", "1"}
	result := getRegionsForUniqueKeys(&intermediatePairs)

	assertIntegersEqual(2, len(result), t)
	assertIntegersEqual(0, result[0].start, t)
	assertIntegersEqual(1, result[0].end, t)
	assertIntegersEqual(1, result[1].start, t)
	assertIntegersEqual(2, result[1].end, t)
}

func TestRegionOfUniqueKeysMultipleDifferentAndSame(t *testing.T) {
	intermediatePairs := make([]KeyValue, 3)
	intermediatePairs[0] = KeyValue{"all", "1"}
	intermediatePairs[1] = KeyValue{"all", "1"}
	intermediatePairs[2] = KeyValue{"two", "1"}
	result := getRegionsForUniqueKeys(&intermediatePairs)

	assertIntegersEqual(2, len(result), t)
	assertIntegersEqual(0, result[0].start, t)
	assertIntegersEqual(2, result[0].end, t)
	assertIntegersEqual(2, result[1].start, t)
	assertIntegersEqual(3, result[1].end, t)
}

func TestRunMapWithHeartbeat(t *testing.T) {
	chunk := "hello world"
	chunks := []string{chunk}

	keyValueResult := runMapWithHeartbeat(chunks, Map)
	assertIntegersEqual(2, len(keyValueResult), t)
	assertStringsEqual("hello", keyValueResult[0].Key, t)
	assertStringsEqual("1", keyValueResult[0].Value, t)
	assertStringsEqual("world", keyValueResult[1].Key, t)
	assertStringsEqual("1", keyValueResult[1].Value, t)
}

func TestSplitStringIntoChunks(t *testing.T) {
	chunk := "hello world this is another test this is another test"
	chunks := splitStringIntoChunks(chunk, 100, " ")
	assertIntegersEqual(10, len(chunks), t)
	assertStringsEqual("hello", chunks[0], t)
	assertStringsEqual("world", chunks[1], t)
	assertStringsEqual("this", chunks[2], t)
	assertStringsEqual("is", chunks[3], t)
	assertStringsEqual("another", chunks[4], t)
	assertStringsEqual("test", chunks[5], t)
	assertStringsEqual("this", chunks[6], t)
	assertStringsEqual("is", chunks[7], t)
	assertStringsEqual("another", chunks[8], t)
	assertStringsEqual("test", chunks[9], t)
}

func TestSplitStringIntoChunksUnevenChunks(t *testing.T) {
	chunk := "hello world this is another test this is another test"
	numChunksRequested := 3
	chunks := splitStringIntoChunks(chunk, numChunksRequested, " ")
	assertIntegersEqual(numChunksRequested, len(chunks), t)
	assertStringsEqual("hello world this", chunks[0], t)
	assertStringsEqual("is another test", chunks[1], t)
	assertStringsEqual("this is another test", chunks[2], t)
}

func assertIntegersEqual(a int, b int, t *testing.T) {
	if a != b {
		t.Errorf("Expected:%d but got:%d", a, b)
	}
}

func assertStringsEqual(a string, b string, t *testing.T) {
	if a != b {
		t.Errorf("Expected:%s but got:%s", a, b)
		t.Errorf("%d, %d", len(a), len(b))
	}
}
