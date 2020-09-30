package main

import (
	"fmt"
	"sort"
	"sync"
)
import "os"

const OutputFileName = "mr-out-0"
const NumberOfMapTasks = 10 // referred to a M in the paper

type MapFunction = func(string) []KeyValue

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrdistributed.go inputfiles...\n")
		os.Exit(1)
	}

	mapFunction := distributedMap //Plugins would be called here

	//Step 1. Read input files and pass content into map
	allContent := aggregateFileContents(os.Args[1:])
	chunks := splitStringIntoChunks(allContent, NumberOfMapTasks)

	intermediateKeyValuePairs := runMapWithHeartbeat(chunks, mapFunction)
	sort.Sort(ByKey(intermediateKeyValuePairs))
	runReduceJobsWithHeartbeat(intermediateKeyValuePairs, OutputFileName)
}

func runMapWithHeartbeat(chunks []string, mapFunction MapFunction) []KeyValue {

	mapChannel := make(chan []KeyValue)
	intermediateKeyValuePairs := []KeyValue{}

	workUnits := make([]WorkUnit, len(chunks))
	for workUnitIndex := 0; workUnitIndex < len(workUnits); workUnitIndex++ {
		chunk := chunks[workUnitIndex]
		workUnit := createMapUnitOfWork(
			chunk,
			mapFunction,
			mapChannel,
			&intermediateKeyValuePairs)
		workUnits[workUnitIndex] = workUnit
	}

	runJobsWithHeartbeat(workUnits)
	close(mapChannel)

	return intermediateKeyValuePairs
}

func createMapUnitOfWork(
	chunk string,
	mapFunction MapFunction,
	mapChannel chan []KeyValue,
	intermediateKeyValuePairs *[]KeyValue) WorkUnit {
	var workUnit WorkUnit
	workUnit.job = func() {
		localWaitGroup := new(sync.WaitGroup)
		localWaitGroup.Add(1)
		go func() {
			mapJob(chunk, mapFunction, mapChannel)
			localWaitGroup.Done()
		}()
		localWaitGroup.Add(1)
		go func() {
			jobResult := <-mapChannel
			for _, keyValuePair := range jobResult {
				*intermediateKeyValuePairs = append(*intermediateKeyValuePairs, keyValuePair)
			}
			localWaitGroup.Done()
		}()
		localWaitGroup.Wait()
	}

	workUnit.cleanup = func() {}
	return workUnit
}

func runReduceJobsWithHeartbeat(
	intermediatePairs []KeyValue,
	outputFilePath string) {

	//1. Listen and handle for reduce jobs outputs
	outputFile, _ := os.Create(outputFilePath)
	reduceChannel := make(chan string)
	go func() {
		for s := range reduceChannel {
			fmt.Fprintf(outputFile, s)
		}
	}()

	//2. Start reduce jobs
	var workUnits = []WorkUnit{}
	regions := getKeyIndices(intermediatePairs)
	for _, region := range regions {
		key := intermediatePairs[region.start].Key
		values := []string{}
		for intermediatePairIndex := region.start; intermediatePairIndex < region.end; intermediatePairIndex++ {
			values = append(values, intermediatePairs[intermediatePairIndex].Value)
		}
		fmt.Print("reducing on key: ", key, "\n")
		workUnit := createReduceWorkUnit(key, values, reduceChannel)
		workUnits = append(workUnits, workUnit)
	}

	runJobsWithHeartbeat(workUnits)
	close(reduceChannel)
}

func getKeyIndices(intermediatePairs []KeyValue) []KeyValueRegion {
	keyStartIndex := 0
	var indices []KeyValueRegion
	for keyStartIndex < len(intermediatePairs) {
		lastKeyIndex := keyStartIndex + 1
		for lastKeyIndex < len(intermediatePairs) && intermediatePairs[lastKeyIndex].Key == intermediatePairs[keyStartIndex].Key {
			lastKeyIndex++
		}
		indices = append(indices, KeyValueRegion{keyStartIndex, lastKeyIndex})
		keyStartIndex = lastKeyIndex
	}
	return indices
}

func createReduceWorkUnit(
	chunkKey string,
	values []string,
	reduceChannel chan string) WorkUnit {

	var workUnit = WorkUnit{}

	workUnit.job = func() {
		wg := new(sync.WaitGroup)
		wg.Add(1)
		go func() {
			reduceJob(chunkKey, values, reduceChannel)
			wg.Done()
		}()
		wg.Wait()
	}
	workUnit.cleanup = func() {}
	return workUnit
}

type KeyValueRegion struct {
	start int
	end   int
}
