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
		intermediateKeyValuePairs := mapFunction(chunk) 
		responseChannel <- intermediateKeyValuePairs
}

func reduceJob(
	jobId int,
	key string,
	values []string,
	responseChannel chan KeyValue,
	wg *sync.WaitGroup) {
		defer wg.Done()
		response := KeyValue{key, strconv.Itoa(len(values))}
		responseChannel <- response
	}