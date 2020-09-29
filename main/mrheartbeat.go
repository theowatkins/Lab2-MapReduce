package main

import (
	"math/rand"
	"time"
)

const MaxNumberOfNeighbors = 3

type Heartbeat struct {
	nodeId  string
	counter int64
}

type HeartbeatTable struct {
	heartbeats []Heartbeat
}

type HeartbeatRelationship struct {
	source int
	target int
}

type ChannelNeighborhood = map[int][] chan Heartbeat

type NeighborhoodAssignments = map[int] []int

func runHeartBeatThread(id string, neighborsChannels [] chan Heartbeat){
	threadHeartbeat := Heartbeat{id, 0}
	for {
		time.Sleep(TimeBetweenHeartbeats)
		threadHeartbeat.counter += 1
		for _, neighborChannel := range neighborsChannels {
			neighborChannel <- threadHeartbeat
		}
	}
}

func createNeighborhood (neighborhoodSize int) ChannelNeighborhood {

	neighborhood := make(ChannelNeighborhood)
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
		numberOfNeighbors := rand.Intn(MaxNumberOfNeighbors)

		for neighborIndex := 0; neighborIndex < numberOfNeighbors; neighborIndex++{
			assignedNeighbor := rand.Intn(neighborhoodSize)
			if !doesRelationshipExist(assignedNeighbor, nodeIndex, relationships) {
				continue
			}
			relationships[nodeIndex] = append(relationships[nodeIndex], assignedNeighbor)
		}
	}
	return relationships
}

func doesRelationshipExist(sourceIndex int, targetIndex int, heartbeatMap NeighborhoodAssignments) bool{
	if _, ok := heartbeatMap[sourceIndex]; ok {
		for relatedId := range heartbeatMap[sourceIndex] {
			if relatedId == targetIndex {
				return true
			}
		}
	}

	if _, ok := heartbeatMap[targetIndex]; ok {
		for relatedId := range heartbeatMap[targetIndex] {
			if relatedId == sourceIndex {
				return true
			}
		}
	}
	return false
}