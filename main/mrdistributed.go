package main

import (
	"fmt"
	"sort"
)
import "os"

const OutputFileName = "mr-out-0"
const NumberOfMapTasks = 2 // referred to a M in the paper

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrdistributed.go inputfiles...\n")
		os.Exit(1)
	}

	mapFunction := Map
	reduceFunction := Reduce

	//Step 1. Read input files and pass content into map
	allContent := aggregateFileContents(os.Args[1:])
	chunks := splitStringIntoChunks(allContent, NumberOfMapTasks)

	intermediateKeyValuePairs := runMapWithHeartbeat(chunks, mapFunction)
	sort.Sort(ByKey(intermediateKeyValuePairs))
	runReduceWithHeartbeat(intermediateKeyValuePairs, reduceFunction, OutputFileName)
}

/*
 * Creates a set of lambdas representing a map job. These lambdas are given to
 * runJobsWithHeartbeat to be created into nodes and run distributedly.
 */
func runMapWithHeartbeat(chunks []string, mapFunction MapFunction) []KeyValue {
	intermediateKeyValuePairs := []KeyValue{}

	workUnits := make([]WorkUnit, len(chunks))
	for workUnitIndex := 0; workUnitIndex < len(workUnits); workUnitIndex++ {
		chunk := chunks[workUnitIndex]
		workUnit := createMapUnitOfWork(
			chunk,
			mapFunction,
			&intermediateKeyValuePairs)
		workUnits[workUnitIndex] = workUnit
	}

	runJobsWithHeartbeat(workUnits)
	return intermediateKeyValuePairs
}

/*
 * Creates a set of lambdas representing the reduce tasks.
 * These tasks are passed into runJobsWithHeartbeat which
 * runs these operations while communication via heartbeat
 */
func runReduceWithHeartbeat(
	intermediatePairs []KeyValue,
	reduceFunction ReduceFunction,
	outputFilePath string) {

	outputFile, _ := os.Create(outputFilePath)
	reduceChannel := make(chan string)

	// 2. Create reduce jobs
	var workUnits = []WorkUnit{}
	regions := getRegionOfUniqueKeys(intermediatePairs)
	for _, region := range regions {
		key := intermediatePairs[region.start].Key
		values := []string{}
		for intermediatePairIndex := region.start; intermediatePairIndex < region.end; intermediatePairIndex++ {
			values = append(values, intermediatePairs[intermediatePairIndex].Value)
		}
		workUnit := createReduceWorkUnit(key, values, reduceFunction, reduceChannel, outputFile)
		workUnits = append(workUnits, workUnit)
	}

	// 3. Run jobs with heartbeat
	runJobsWithHeartbeat(workUnits)
	close(reduceChannel)
}
