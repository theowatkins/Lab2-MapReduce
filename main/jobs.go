package main

import "sync"

func mapJob(
	jobId int,
	chunk string,
	mapFunction func (filename string, contents string) []KeyValue,
	responseChannel chan []KeyValue,
	wg *sync.WaitGroup) {
	intermediateKeyValuePairs := mapFunction("", chunk) // TODO: Actually give it the file name
	responseChannel <- intermediateKeyValuePairs
	wg.Done()
}
