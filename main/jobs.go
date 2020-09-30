package main

import (
	"strconv"
	"time"
)

func mapJob(
	chunk string,
	mapFunction func(contents string) []KeyValue,
	responseChannel chan []KeyValue) {
	var intermediateKeyValuePairs []KeyValue
	time.Sleep(time.Second * 5)
	intermediateKeyValuePairs = mapFunction(chunk)
	responseChannel <- intermediateKeyValuePairs
}

func reduceJob(
	key string,
	values []string,
	responseChannel chan string) {
	response := key + " " + strconv.Itoa(len(values)) + "\n"
	responseChannel <- response
}
