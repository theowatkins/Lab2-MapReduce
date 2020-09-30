package main

import (
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"time"
)

const MaxNumberOfNeighbors = 1
const TimeBetweenHeartbeats = time.Second

type Heartbeat struct {
	nodeId  string
	counter int64
}

type HeartbeatTable struct {
	heartbeats []Heartbeat
}

type NodeChannels = map[string][] chan Heartbeat
type NeighborChannels = [] chan Heartbeat
type NeighborhoodAssignments = map[string] []string

func runHeartBeatThread(
	id string,
	neighborhoodChannels NeighborChannels,
	quitChannel chan bool,
	parentWg * sync.WaitGroup) {
	defer parentWg.Done()

	wg := new(sync.WaitGroup)
	threadHeartbeat := Heartbeat{id, 0}
	aggregateChannel := make(chan Heartbeat)
	isAlive := true

	//listener
	wg.Add(1)
	go func(){
		for isAlive {
			fmt.Print("is alive")
			for _, neighborhoodChannel := range neighborhoodChannels {
				neighborhoodChannel := neighborhoodChannel
				select {
					case _, ok := <- neighborhoodChannel :
						if ok {
							//aggregateChannel <- newValue
						}
					default: break

				}
			}
			time.Sleep(time.Second)
		}
		fmt.Print("heart beat listener exiting....")
		wg.Done()
	}()

	wg.Add(1)
	go func(){
		for isAlive {
			time.Sleep(time.Second)
			processNode(&threadHeartbeat,
				&neighborhoodChannels[0],
				&neighborhoodChannels,
				&aggregateChannel)
		}
		wg.Done()
	}()

	wg.Add(1)
	go func(){
		for isAlive {
			select {
				case heartbeatUpdate := <- aggregateChannel:
					fmt.Print("Job ", id, " received update: ", heartbeatUpdate, "\n")
			}
		}
		wg.Done()
	}()

	//quit handler
	wg.Add(1)
	go func(){
		select {
			case <- quitChannel:
				isAlive = false
				fmt.Print("Job", id, " received exit signal.\n")
				wg.Done()
		}
	}()

	wg.Wait()
}

func processNode(
	heartbeat * Heartbeat,
	sendChannelRef * chan Heartbeat,
	neighborhoodChannels * NeighborChannels,
	aggregateChannel * chan Heartbeat){

	wg := new(sync.WaitGroup)
	wg.Add(1)

	//update and send heartbeat
	go func(){

	}()

	wg.Add(1)

	wg.Wait()
	fmt.Print("DONE\nDONE\n")
}


func updateHeartbeatTable(table * HeartbeatTable, update Heartbeat) {
	for _, entry := range table.heartbeats {
		if update.nodeId == entry.nodeId {
			if update.counter > entry.counter {
				entry.counter = update.counter
			} else if update.counter == entry.counter {
				//TODO: Mark in danger
			} else {
				//TODO: Mark dead
			}
		}
	}
}

func createNeighborhood (neighborhoodSize int) NodeChannels {

	neighborhood := make(NodeChannels, neighborhoodSize)
	assignments := createRandomNeighborhoodAssignments(neighborhoodSize)

	for sourceId, sourceAssignments := range assignments {
		for _, sourceAssignmentId := range sourceAssignments {
			assignmentChannel := make(chan Heartbeat)
			neighborhood[sourceId] = append(neighborhood[sourceId], assignmentChannel)
			neighborhood[sourceAssignmentId] = append(neighborhood[sourceId], assignmentChannel)
		}
	}

	return neighborhood
}


func createRandomNeighborhoodAssignments(neighborhoodSize int) NeighborhoodAssignments {
	relationships := make(NeighborhoodAssignments)
	for nodeIndex :=0; nodeIndex < neighborhoodSize; nodeIndex++ {
		nodeId := generateNodeId(nodeIndex)
		relationships[nodeId] = []string{}
		numberOfNeighbors := rand.Intn(MaxNumberOfNeighbors) + 1 //Intn gives index

		for neighborIndex := 0; neighborIndex < numberOfNeighbors; neighborIndex++{
			assignedNeighbor := generateNodeId(rand.Intn(neighborhoodSize))
			if isNewRelationship(assignedNeighbor, nodeId, relationships) {
				relationships[nodeId] = append(relationships[nodeId], assignedNeighbor)
			}
		}
	}
	return relationships
}

func generateNodeId(nodeIndex int) string{
	return strconv.Itoa(nodeIndex)
}


func isNewRelationship(sourceId string, targetId string, heartbeatMap NeighborhoodAssignments) bool{
	for _, sourceNeighborId := range heartbeatMap[sourceId] {
		if sourceNeighborId == targetId {
			return false
		}
	}

	for _, targetNeighborId := range heartbeatMap[targetId] {
		if targetNeighborId == sourceId {
			return false
		}
	}
	return true
}