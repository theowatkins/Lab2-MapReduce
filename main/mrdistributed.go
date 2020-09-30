package main

import (
	"fmt"
	"sort"
)
import "os"

const OutputFileName = "mr-out-0"
const NumberOfMapTasks = 1 // referred to a M in the paper

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
	runReduceWithHeartbeat(&intermediateKeyValuePairs, reduceFunction, OutputFileName)
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
	intermediatePairsRef *[]KeyValue,
	reduceFunction ReduceFunction,
	outputFilePath string) {

	outputFile, _ := os.Create(outputFilePath)

	// 2. Create reduce jobs
	var workUnits = []WorkUnit{}
	intermediatePairs := *intermediatePairsRef
	regions := getRegionOfUniqueKeys(intermediatePairsRef)

	for _, region := range regions {
		key := intermediatePairs[region.start].Key
		keys := []string{}
		values := []string{}
		for intermediatePairIndex := region.start; intermediatePairIndex < region.end; intermediatePairIndex++ {
			values = append(values, intermediatePairs[intermediatePairIndex].Value)
			keys = append(keys, intermediatePairs[intermediatePairIndex].Key)
		}
		workUnit := createReduceWorkUnit(key, values, reduceFunction, outputFile)
		workUnits = append(workUnits, workUnit)

	}

	// 3. Run jobs with heartbeat
	runJobsWithHeartbeat(workUnits)
}
