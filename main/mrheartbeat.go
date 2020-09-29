package main

import (
	"fmt"
	"math/rand"
	"strconv"
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
	quit chan bool){
	//threadHeartbeat := Heartbeat{id, 0}
	//threadTable := HeartbeatTable{make([]Heartbeat, 1)}
	fmt.Print("Running heartbeat for ", id, "\n")
	//for {
	//	timeToUpdate := true
	//	fmt.Print(timeToUpdate)
	//	for _, neighborhoodChannel := range neighborhoodChannels {
	//		select {
	//			case <- quit:
	//				timeToUpdate = false
	//				fmt.Print("Quitting Job: ", id, "\n")
	//				return
	//			case newMessage := <- neighborhoodChannel:
	//				updateHeartbeatTable(&threadTable, newMessage)
	//			default:
	//				if timeToUpdate {
	//					fmt.Print("Updating Job: ", id, "\n")
	//					timeToUpdate = false
	//					threadHeartbeat.counter += 1
	//					neighborhoodChannel <- threadHeartbeat
	//					time.Sleep(TimeBetweenHeartbeats)
	//					timeToUpdate = true
	//				}
	//			}
	//	}
	//}
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