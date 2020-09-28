package main

import (
	"strconv"
	"sync"
)

func mapJob(
	jobId int,
	chunk string,
	mapFunction func (contents string) []KeyValue,
	responseChannel chan []KeyValue,
	wg *sync.WaitGroup) {
		defer wg.Done()
		//TODO: Implement Heartbeat protocol lol
		intermediateKeyValuePairs := mapFunction(chunk) // TODO: Actually give it the file name
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