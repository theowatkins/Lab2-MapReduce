package main

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestGetAggregateContentInFiles(t *testing.T){
	var fileNames = []string{
		"../testResources/testFileOne.txt",
		"../testResources/testFileTwo.txt"}

	content := aggregateFileContents(fileNames)
	var expected = "hello world this is another test"
	if content != expected {
		t.Errorf("Expected %s but got %s", expected, content)
	}
}

func TestHeartBeat(t *testing.T){
	channel := make(HeartbeatChannel)
	channels := HeartbeatChannels{channel}

	var wg = sync.WaitGroup{}
	wg.Add(2)
	go performWorkWithHeartbeat(1,channels, func (){
		time.Sleep(time.Second * 2)
		fmt.Print("Starting job A...\n")
	}, func () {
		fmt.Print("Cleaning A.\n")
		wg.Done()
	})

	go performWorkWithHeartbeat(2, channels, func () {
		time.Sleep(time.Second * 2)
		fmt.Print("Starting job B...\n")
	}, func() {
		fmt.Print("Cleaning B.\n")
		wg.Done()
	})

	wg.Wait()
	fmt.Print("Closing channels")
	close(channel)
	fmt.Print("Testing done")
}