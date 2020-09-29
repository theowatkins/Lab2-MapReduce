package main

import (
	"strconv"
	"sync"
	"time"
)

const TimeBetweenHeartbeats = 2 * time.Second

func mapJob(
	jobId int,
	chunk string,
	mapFunction func (contents string) []KeyValue,
	responseChannel chan []KeyValue,
	wg *sync.WaitGroup,
	neighborsChannels [] chan Heartbeat) {
		defer wg.Done()
		//TODO: Implement Heartbeat protocol lol
		var intermediateKeyValuePairs []KeyValue
		var workTask sync.WaitGroup
		workTask.Add(1)
		go func () {
			intermediateKeyValuePairs = mapFunction(chunk) // TODO: Actually give it the file name
		}()
		quiteHeartbeat := make(chan bool)
		go func () {
			// listen for change in channel
			for {
				select {
				case <-quiteHeartbeat:
					return
				default:
					runHeartBeatThread(string(jobId), neighborsChannels )
				}
			}
		}()
		workTask.Wait()
		quiteHeartbeat <- true
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