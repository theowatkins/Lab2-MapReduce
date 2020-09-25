package main

import "testing"

func TestGetAggregateContentInFiles(t *testing.T){
	var fileNames = []string{
		"../testResources/testFileOne.txt",
		"../testResources/testFileTwo.txt"}

	content := aggregateFileContents(fileNames)
	var expected = "hello world this is another test"
	if content != expected {
		t.Errorf("Expected %s but got %s", expected, content)
	}
}