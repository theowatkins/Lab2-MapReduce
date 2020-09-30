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
 * UnitOfWork - Lambdas representing some work that will get parallelized
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
	outputFile *os.File) WorkUnit {

	var workUnit = WorkUnit{}

	//TODO : Because of the random order of execution of goroutines output is not sorted.
	workUnit.work = func() {
		s := reduceJob(chunkKey, values, reduceFunction)
		fmt.Fprintf(outputFile, s)
	}

	return workUnit
}

//Returns a list of regions (start + end indices) marking where each key lies in the given reference.
func getRegionsForUniqueKeys(intermediatePairsRef *[]KeyValue) []KeyValueRegion {
	intermediatePairs := * intermediatePairsRef
	keyStartIndex := 0
	var indices []KeyValueRegion
	for keyStartIndex < len(intermediatePairs) {
		nextKeyIndex := keyStartIndex + 1
		for nextKeyIndex < len(intermediatePairs) &&
			intermediatePairs[nextKeyIndex].Key == intermediatePairs[keyStartIndex].Key {
			nextKeyIndex++
		}
		indices = append(indices, KeyValueRegion{keyStartIndex, nextKeyIndex})
		keyStartIndex = nextKeyIndex
	}
	return indices
}
