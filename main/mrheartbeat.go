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
	quit chan bool,
	wg * sync.WaitGroup) {
	fmt.Print("Starting new heart beat thread!")
	threadHeartbeat := Heartbeat{id, 0}
	threadTable := HeartbeatTable{make([]Heartbeat, 1)}
	isRunning := true

	wg.Add(2)
	//listener
	go func() {
		for _, neighborhoodChannel := range neighborhoodChannels {
			select {
				case <-quit:
					fmt.Print("Quitting")
					isRunning = false
					wg.Done()
					return
				case newMessage := <-neighborhoodChannel:
					updateHeartbeatTable(&threadTable, newMessage)
				default:
			}
		}
	}()
	go func() {
		for {
			timeToUpdate := true
			if !isRunning {
				wg.Done()
				return
			}
			if timeToUpdate {
				timeToUpdate = false
				threadHeartbeat.counter += 1
				randomNeighborIndex := rand.Intn(len(neighborhoodChannels))
				neighborhoodChannels[randomNeighborIndex] <- threadHeartbeat
				time.Sleep(TimeBetweenHeartbeats)
				timeToUpdate = true
			}
		}
	}()
	wg.Wait()
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