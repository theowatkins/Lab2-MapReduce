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

	mapFunction := distributedMap //Plugins would be called here

	//Step 1. Read input files and pass content into map
	allContent := aggregateFileContents(os.Args[1:])
	chunks := splitStringIntoChunks(allContent, NumberOfMapTasks)

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

	// for _, currentChannel := range mapResponseChannels {
	// 	currentChannelKeyValuePairs := <- currentChannel
	// 	intermediateKeyValuePairs = append(intermediateKeyValuePairs, currentChannelKeyValuePairs...)
	// }

	//Step 4. Create input chunks for reduce workers
	//TODO: Create R chunks, one for each reduce task
	sort.Sort(ByKey(intermediateKeyValuePairs))

	//Step 5. Reduce each chunk and aggregate output
	//TODO: Create many reduce tasks
	reduceChannel := make(chan string)
	reduceIntermediateKeyValuePairs(intermediateKeyValuePairs, reduceJob, OutputFileName, reduceChannel, wg)
}

/* Calls given reduceFunction on given intermediate key-value pairs
 * Outputs is saved to given file path. Note, this is part of the original
 * code given to us.
 */
func reduceIntermediateKeyValuePairs(
	intermediatePairs []KeyValue,
	reduceJob func(jobId int,
		key string,
		values []string,
		responseChannel chan string,
		wg *sync.WaitGroup),
	outputFilePath string,
	reduceChannel chan string,
	wg *sync.WaitGroup) {

	outputFile, _ := os.Create(outputFilePath)

	i := 0
	jobCount := 0
	for i < len(intermediatePairs) {
		j := i + 1
		for j < len(intermediatePairs) && intermediatePairs[j].Key == intermediatePairs[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediatePairs[k].Value)
		}
		wg.Add(1)
		go reduceJob(jobCount, intermediatePairs[i].Key, values, reduceChannel, wg)
		jobCount++
		i = j
	}
	
	go func(wg *sync.WaitGroup, reduceChannel chan string) {
		wg.Wait()
		close(reduceChannel)
	}(wg, reduceChannel)

	for s := range reduceChannel {
		fmt.Fprintf(outputFile, s)
	}

	// for z := 0; z < jobCount; z++ {
	// 	currentChannelOutput := <- reduceChannel
	// 	fmt.Fprintf(outputFile, currentChannelOutput)
	// } 
	outputFile.Close()
}

