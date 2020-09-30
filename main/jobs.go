package main

import (
	"strconv"
	"sync"
	"time"
)

func mapJob(
	jobId int,
	chunk string,
	mapFunction func (contents string) []KeyValue,
	responseChannel chan []KeyValue,
	wg *sync.WaitGroup,
	neighborsChannel NeighborChannels) {
		defer wg.Done()
		var intermediateKeyValuePairs []KeyValue
		var workTask sync.WaitGroup
		workTask.Add(1)
		go func () {
			time.Sleep(time.Second * 5)
			intermediateKeyValuePairs = mapFunction(chunk) // TODO: Actually give it the file name
			workTask.Done()
		}()

		intermediateKeyValuePairs = mapFunction(chunk)
		workTask.Wait() // job has completed
		responseChannel <- intermediateKeyValuePairs
}


func reduceJob(
	jobId int,
	key string,
	values []string,
	responseChannel chan string,
	wg *sync.WaitGroup) {
		defer wg.Done()
		response := key + " " + strconv.Itoa(len(values)) + "\n"
		responseChannel <- response
	}