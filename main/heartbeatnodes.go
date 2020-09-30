package main

import (
	"math/rand"
	"strconv"
)

/*
 * This file contains the functions that create the neighborhood for nodes of heartbeat protocol.
 */
func createNeighborhood(neighborhoodSize int) HeartbeatChannelMap {

	neighborhood := make(HeartbeatChannelMap, neighborhoodSize)
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

func createRandomNeighborhoodAssignments(neighborhoodSize int) NeighborAssignments {
	relationships := make(NeighborAssignments)
	for nodeIndex := 0; nodeIndex < neighborhoodSize; nodeIndex++ {
		nodeId := generateNodeId(nodeIndex)
		relationships[nodeId] = []string{}
		numberOfNeighbors := rand.Intn(MaxNumberOfNeighbors) + 1 //Intn gives index

		for neighborIndex := 0; neighborIndex < numberOfNeighbors; neighborIndex++ {
			assignedNeighbor := generateNodeId(rand.Intn(neighborhoodSize))
			if isNewRelationship(assignedNeighbor, nodeId, relationships) {
				relationships[nodeId] = append(relationships[nodeId], assignedNeighbor)
			}
		}
	}
	return relationships
}

func generateNodeId(nodeIndex int) string {
	return strconv.Itoa(nodeIndex)
}

func isNewRelationship(sourceId string, targetId string, heartbeatMap NeighborAssignments) bool {
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
