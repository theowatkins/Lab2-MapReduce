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
	neighborChannels NeighborChannels) {
	var intermediateKeyValuePairs []KeyValue

	performWorkWithHeartbeat(
		jobId,
		neighborChannels,
		func() {
			//time.Sleep(time.Second * 5)
			intermediateKeyValuePairs = mapFunction(chunk)
		}, func() {
			responseChannel <- intermediateKeyValuePairs
			fmt.Print(jobId, " mapJob finished.")
			defer wg.Done()
		})
}

func performWorkWithHeartbeat (
	jobId int,
	neighborsChannel NeighborChannels,
	work func (),
	cleanup func()){
	var workTask sync.WaitGroup


	//Begin heartbeat
	quitChannel := make(chan bool)
	workTask.Add(1)
	go runHeartBeatThread(generateNodeId(jobId), neighborsChannel, quitChannel, &workTask)

	//Do Work
	workTask.Add(1)
	go func () {
		fmt.Print("Starting work\n")
		work()
		workTask.Done()
		quitChannel <- true	//Stop heartbeat
		fmt.Print("End work\n")
	}()

	workTask.Wait() // job and heart beat stopped
	cleanup()
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