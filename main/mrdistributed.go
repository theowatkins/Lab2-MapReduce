package main

import (
	"fmt"
	"sync"
)
import "os"
import "sort"

const OutputFileName = "mr-out-0"
const NumberOfMapTasks = 100 // referred to a M in the paper

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrdistributed.go inputfiles...\n")
		os.Exit(1)
	}

	mapFunction, reduceFunction := distributedMap, distributedReduce //Plugins would be called here

	//Step 1. Read input files and pass content into map
	allContent := aggregateFileContents(os.Args[1:])
	chunks := splitStringIntoChunks(allContent, NumberOfMapTasks)

	//Step 2. Start map workers
	mapResponseChannels := createInitializedChannelList(NumberOfMapTasks)
	var wg sync.WaitGroup
	for chunkId, chunk := range chunks {
		wg.Add(1)
		go mapJob(chunkId, chunk, mapFunction,mapResponseChannels[chunkId], &wg)
	}
	wg.Wait() //wait for all map tasks to finish

	//Step 3. Buffer intermediate pairs into memory
	intermediateKeyValuePairs := []KeyValue{}
	for _, currentChannel := range mapResponseChannels {
		currentChannelKeyValuePairs := <- currentChannel
		intermediateKeyValuePairs = append(intermediateKeyValuePairs, currentChannelKeyValuePairs...)
	}

	//Step 4. Create input chunks for reduce workers
	//TODO: Create R chunks, one for each reduce task
	sort.Sort(ByKey(intermediateKeyValuePairs))

	//Step 5. Reduce each chunk and aggregate output
	//TODO: Create many reduce tasks
	reduceIntermediateKeyValuePairs(intermediateKeyValuePairs, reduceFunction, OutputFileName)
}

/* Calls given reduceFunction on given intermediate key-value pairs
 * Outputs is saved to given file path. Note, this is part of the original
 * code given to us.
 */
func reduceIntermediateKeyValuePairs(
	intermediatePairs []KeyValue,
	reduceFunction func(key string, values []string) string,
	outputFilePath string,
	){

	outputFile, _ := os.Create(outputFilePath)

	i := 0
	for i < len(intermediatePairs) {
		j := i + 1
		for j < len(intermediatePairs) && intermediatePairs[j].Key == intermediatePairs[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediatePairs[k].Value)
		}
		output := reduceFunction(intermediatePairs[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(outputFile, "%v %v\n", intermediatePairs[i].Key, output)

		i = j
	}

	outputFile.Close()
}

