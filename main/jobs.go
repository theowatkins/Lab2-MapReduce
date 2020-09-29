package main

import (
	"fmt"
	"strconv"
	"sync"
)

func mapJob(
	jobId int,
	chunk string,
	mapFunction func (contents string) []KeyValue,
	responseChannel chan []KeyValue,
	wg *sync.WaitGroup,
	neighborsChannel NeighborChannels) {
		defer wg.Done()
		//TODO: Implement Heartbeat protocol lol
		var intermediateKeyValuePairs []KeyValue
		//var workTask sync.WaitGroup
		//workTask.Add(1)
		//go func () {
		//	intermediateKeyValuePairs = mapFunction(chunk) // TODO: Actually give it the file name
		//	workTask.Done()
		//}()
		//quitHeartbeat := make(chan bool)
		//go runHeartBeatThread(generateNodeId(jobId), neighborsChannel, quitHeartbeat)
		intermediateKeyValuePairs = mapFunction(chunk)
		//workTask.Wait() // job has completed
		fmt.Print("Job finished: ", jobId, "\n")
		//quitHeartbeat <- true
		fmt.Print("closing quite heartbeat for Job: ", jobId, "\n")
		responseChannel <- intermediateKeyValuePairs
		fmt.Print("Job ", jobId, " done!\n")
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