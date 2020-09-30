package main

import (
	"fmt"
	"os"
)

type WorkUnit struct {
	work func()
}

type KeyValueRegion struct {
	start int
	end   int
}

/*
 * Jobs - synchronous user-defined operations
 */
func mapJob(
	chunk string,
	mapFunction func(contents string) []KeyValue) []KeyValue {
	var intermediateKeyValuePairs []KeyValue
	intermediateKeyValuePairs = mapFunction(chunk)
	return intermediateKeyValuePairs
}

func reduceJob(
	key string,
	values []string,
	reduceFunction ReduceFunction) string {
	response := key + " " + reduceFunction(key, values) + "\n"
	return response
}

/*
 * UnitOfWork - Lambdas representing the work necessary to parallelize operations
 */
func createMapUnitOfWork(
	chunk string,
	mapFunction MapFunction,
	intermediateKeyValuePairs *[]KeyValue) WorkUnit {
	var workUnit WorkUnit
	workUnit.work = func() {
		jobResult := mapJob(chunk, mapFunction)
		for _, keyValuePair := range jobResult {
			*intermediateKeyValuePairs = append(*intermediateKeyValuePairs, keyValuePair)
		}
	}

	return workUnit
}

func createReduceWorkUnit(
	chunkKey string,
	values []string,
	reduceFunction ReduceFunction,
	reduceChannel chan string,
	outputFile *os.File) WorkUnit {

	var workUnit = WorkUnit{}

	workUnit.work = func() {
		s := reduceJob(chunkKey, values, reduceFunction)
		fmt.Fprintf(outputFile, s)
	}

	return workUnit
}

/*
 * Utilities
 */

func getRegionOfUniqueKeys(intermediatePairs []KeyValue) []KeyValueRegion {
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
