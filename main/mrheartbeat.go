package main

import (
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

type HeartbeatChannelMap = map[string][]chan Heartbeat
type HeartbeatChannel = chan Heartbeat
type HeartbeatChannels = []HeartbeatChannel
type NeighborAssignments = map[string][]string

type WorkUnit struct {
	job     func()
	cleanup func()
}

func runJobsWithHeartbeat(workUnits []WorkUnit) {
	numberOfJobs := len(workUnits)
	neighborsChannels := createNeighborhood(numberOfJobs)
	var wg sync.WaitGroup

	for workIndex, work := range workUnits {
		wg.Add(1)
		go func() {
			performWorkWithHeartbeat(
				workIndex,
				neighborsChannels[generateNodeId(workIndex)],
				work.job,
				work.cleanup)
			wg.Done()
		}()
	}
	wg.Wait()

	channelMap := make(map[chan Heartbeat]int)

	for _, channelGroup := range neighborsChannels {
		for _, channel := range channelGroup {
			if _, ok := channelMap[channel]; ok {
				//do nothing
			} else {
				channelMap[channel] = 1
				close(channel)
			}
		}
	}
}

func performWorkWithHeartbeat(
	jobId int,
	neighborsChannel HeartbeatChannels,
	work func(),
	cleanup func()) {

	var workTask sync.WaitGroup
	//Begin heartbeat
	quitChannel := make(chan bool)
	go runHeartBeatThread(generateNodeId(jobId), neighborsChannel, quitChannel)

	//Do Work
	workTask.Add(1)
	go func() {
		work()
		workTask.Done()
		quitChannel <- true //Stop heartbeat
	}()

	workTask.Wait() // job and heart beat stopped
	cleanup()
}

func runHeartBeatThread(
	id string,
	neighborhoodChannels HeartbeatChannels,
	quitChannel chan bool) {
	wg := new(sync.WaitGroup)
	threadHeartbeat := Heartbeat{id, 0}
	aggregateChannel := make(chan Heartbeat)
	isAlive := true
	table := HeartbeatTable{[]Heartbeat{}}

	/*
	 * Main heart beat thread
	 */
	wg.Add(1)
	go func() {
		for isAlive {
			time.Sleep(TimeBetweenHeartbeats)
			if isAlive { //function could have exited, do not touch state
				heartbeatTick(&threadHeartbeat, &neighborhoodChannels)
			}
		}
		wg.Done()
	}()

	/*
	 * Aggregates communications channels into single channel
	 */
	go func() {
		for isAlive && len(neighborhoodChannels) > 0 {
			select {
			case newValue, ok := <-neighborhoodChannels[0]:
				if !ok {
					return
				}
				aggregateChannel <- newValue
			}
		}
	}()

	/*
	 * On Update listener
	 */
	wg.Add(1)
	go func() {
		for isAlive {
			select {
			case heartbeatUpdate := <-aggregateChannel:
				updateHeartbeatTable(&table, heartbeatUpdate)
			default:
			}
		}
		wg.Done()
	}()

	/*
	 * Quit handler
	 */
	wg.Add(1)
	go func() {
		select {
		case <-quitChannel:
			isAlive = false
			wg.Done()
		}
	}()

	wg.Wait()
}

func heartbeatTick(
	heartbeat *Heartbeat,
	neighborhoodChannels *HeartbeatChannels) {
	heartbeat.counter += 1
	if len(*neighborhoodChannels) > 0 {
		(*neighborhoodChannels)[0] <- *heartbeat
	}
}

func updateHeartbeatTable(table *HeartbeatTable, update Heartbeat) {
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
