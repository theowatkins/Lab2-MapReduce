package main

import (
	"fmt"
	"strconv"
	"sync"
	"time"
)

func mapJob(
	jobId int,
	chunk string,
	mapFunction func (contents string) []KeyValue,
	responseChannel chan []KeyValue,
	neighborChannels HeartbeatChannels) {
	var intermediateKeyValuePairs []KeyValue
	performWorkWithHeartbeat(
		jobId,
		neighborChannels,
		func() {
			time.Sleep(time.Second * 5)
			intermediateKeyValuePairs = mapFunction(chunk)
		}, func() {
			responseChannel <- intermediateKeyValuePairs
			fmt.Print(jobId, " mapJob finished.")
		})
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