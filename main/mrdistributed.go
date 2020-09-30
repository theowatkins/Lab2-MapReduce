package main

import (
	"fmt"
	"sort"
	"sync"
)
import "os"

const OutputFileName = "mr-out-0"
const NumberOfMapTasks = 10 // referred to a M in the paper

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
	mapChannel := make(chan []KeyValue)
	var wg sync.WaitGroup
	neighborsChannels := createNeighborhood(NumberOfMapTasks)
	intermediateKeyValuePairs := []KeyValue{}

	var workUnits = [NumberOfMapTasks]WorkUnit{}
	for workUnitIndex :=0 ; workUnitIndex < len(workUnits); workUnitIndex++{
		chunk := chunks[workUnitIndex]
		job := func (){
			wg := new(sync.WaitGroup)
			wg.Add(1)
			go func(){
				mapJob(workUnitIndex, chunk, mapFunction, mapChannel, neighborsChannels[generateNodeId(chunkId)])
				wg.Done()
			}()
			wg.Add(1)
			go func () {
				jobResult := <- mapChannel
				for _, keyValuePair := range jobResult {
					intermediateKeyValuePairs = append(intermediateKeyValuePairs, keyValuePair)
				}
				wg.Done()
			}()
			wg.Wait()
		}
		cleanup := func () {}
		workUnits[workUnitIndex] = WorkUnit{job, cleanup}
	}


	for chunkId, chunk := range chunks {
		wg.Add(1)
		go func(){
			mapJob(chunkId, chunk, mapFunction, mapChannel, neighborsChannels[generateNodeId(chunkId)])
			wg.Done()
		}()
		wg.Add(1)
		go func () {
			jobResult := <- mapChannel
			for _, keyValuePair := range jobResult {
				intermediateKeyValuePairs = append(intermediateKeyValuePairs, keyValuePair)
			}
			wg.Done()
		}()
	}
	wg.Wait()

	channelMap := make(map[chan Heartbeat]int)

	fmt.Print("close channels...\n")
	for _, channelGroup := range neighborsChannels {
		for _, channel := range channelGroup {
			if _, ok := channelMap[channel]; ok {
				//do nothing
			} else {
				channelMap[channel] = 1
				close(channel)
			}
		}
	}
	fmt.Print("Closing map channels\n")
	close(mapChannel)

	//Step 4. Create input chunks for reduce workers
	//TODO: Create R chunks, one for each reduce task
	sort.Sort(ByKey(intermediateKeyValuePairs))

	//Step 5. Reduce each chunk and aggregate output
	//TODO: Create many reduce tasks
	reduceChannel := make(chan string)
	reduceIntermediateKeyValuePairs(intermediateKeyValuePairs, reduceJob, OutputFileName, reduceChannel, &wg)
	fmt.Print("Done!")
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

