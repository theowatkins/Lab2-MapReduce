package main

import (
	"fmt"
	"sync"
)
import "os"
import "sort"

const OutputFileName = "mr-distributed-out"
const NumberOfMapTasks = 100 // referred to as M in the paper

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrdistributed.go inputfiles...\n")
		os.Exit(1)
	}

	//Plugins would be called here
	mapFunction, reduceFunction := distributedMap, distributedReduce 

	//Step 1. Read input files and pass content into map
	allContent := aggregateFileContents(os.Args[1:])
	chunks := splitStringIntoChunks(allContent, NumberOfMapTasks, " ")

	//Step 2. Start map workers
	//mapResponseChannels := createInitializedChannelList(NumberOfMapTasks)
	mapChannel := make(chan []KeyValue)
	wg := new(sync.WaitGroup)
	for chunkId, chunk := range chunks {
		wg.Add(1)
		go mapJob(chunkId, chunk, mapFunction, mapChannel, wg)
	}
	go func(wg *sync.WaitGroup, mapChannel chan []KeyValue) {
		wg.Wait()
		close(mapChannel)
	}(wg, mapChannel)

	//Step 3. Buffer intermediate pairs into memory
	intermediateKeyValuePairs := []KeyValue{}
	
	for i := range mapChannel {
		for j := 0; j < len(i); j++ {
			intermediateKeyValuePairs = append(intermediateKeyValuePairs, i[j])
		}
	}

	//Step 4. Create input chunks for reduce workers
	sort.Sort(ByKey(intermediateKeyValuePairs))

	//Step 5. Reduce each chunk and aggregate output
	reduceChannel := make(chan KeyValue)
	reduceFunction(intermediateKeyValuePairs, reduceJob, OutputFileName, reduceChannel, wg)
}



