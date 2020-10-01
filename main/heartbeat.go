package main

import (
	"sync"
	"time"
)

const MaxNumberOfNeighbors = 1
const TimeBetweenHeartbeats = time.Second

type Heartbeat struct {
	nodeId  string
	counter int64
	troubleCounter int
}

type HeartbeatTable struct {
	heartbeats []Heartbeat
}

type HeartbeatChannel chan Heartbeat
type HeartbeatChannels []HeartbeatChannel
type HeartbeatChannelMap map[string]HeartbeatChannels
type NeighborAssignments map[string][]string

func runJobsWithHeartbeat(workUnits []WorkUnit) {
	numberOfJobs := len(workUnits)
	neighborsChannels := createNeighborhood(numberOfJobs)
	wg := new(sync.WaitGroup)

	for workIndex, work := range workUnits {
		wg.Add(1)
		workIndex := workIndex
		work := work
		go func() {
			performJobWithHeartbeat(
				workIndex,
				neighborsChannels[generateNodeId(workIndex)],
				work.work)
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

/*
 * Assumes the job you are accomplishing has two tasks:
 * 1. Do some operation and writing to a channel
 * 2. Listen to the output of that channel and doing something with it.
 */
func performJobWithHeartbeat(
	jobId int,
	neighborsChannel HeartbeatChannels,
	job func()) {

	jobWaitGroup := new(sync.WaitGroup)
	quitHeartbeatChannel := make(chan bool)

	//1. Begin heartbeat
	jobWaitGroup.Add(1)
	go func() {
		runHeartBeatThread(generateNodeId(jobId), neighborsChannel, quitHeartbeatChannel)
		close(quitHeartbeatChannel)
		jobWaitGroup.Done()
	}()

	//2. Do job
	jobWaitGroup.Add(1)
	go func() {
		job()
		quitHeartbeatChannel <- true
		jobWaitGroup.Done()
	}()

	jobWaitGroup.Wait() // job and heart beat stopped
}

const NeighborToSendTo = 0

func runHeartBeatThread(
	threadId string,
	neighborhoodChannels HeartbeatChannels,
	quitChannel chan bool) {

	threadWaitGroup := new(sync.WaitGroup)
	threadHeartbeat := Heartbeat{threadId, 0, 0}
	theadAggregateChannel := make(chan Heartbeat)
	theadIsAlive := true
	threadHeartbeatTable := HeartbeatTable{[]Heartbeat{}}

	/*
	 * Heart beat thread - Increments counter and sends to neighbor when
	 */
	threadWaitGroup.Add(1)
	go func() {
		for theadIsAlive {
			time.Sleep(TimeBetweenHeartbeats)
			if theadIsAlive { //function could have exited when sleeping, do NOT touch state
				heartbeatTick(&threadHeartbeat, &neighborhoodChannels)
			}
		}
		threadWaitGroup.Done()
	}()

	/*
	 * On Neighbor Update Handler - Aggregates communications channels from neighbors into single channel.
	 * Note, because we are always sending to the same neighbor some listeners on neighborhood channels will
	 * never receiver a value. This will leave the threads hanging until the channel closes. This is why
	 * this handler contains no wait group.
	 */
	go func() {
		for theadIsAlive && len(neighborhoodChannels) > 0 {
			select {
			case newValue, ok := <-neighborhoodChannels[NeighborToSendTo]:
				if !ok {
					return
				}
				theadAggregateChannel <- newValue
			}
		}
	}()

	/*
	 * On Update listener - Processes heartbeats sent to node and updates table
	 */
	threadWaitGroup.Add(1)
	go func() {
		for theadIsAlive {
			select {
			case heartbeatUpdate := <-theadAggregateChannel:
				updateHeartbeatTable(&threadHeartbeatTable, heartbeatUpdate, quitChannel)
			default:
			}
		}
		threadWaitGroup.Done()
	}()

	/*
	 * Quit handler - waits for process to quit and sets flag.
	 */
	threadWaitGroup.Add(1)
	go func() {
		select {
		case <-quitChannel:
			theadIsAlive = false
		}
		threadWaitGroup.Done()
	}()

	threadWaitGroup.Wait()
}

func heartbeatTick(
	heartbeat *Heartbeat,
	neighborhoodChannels *HeartbeatChannels) {
	heartbeat.counter += 1
	if len(*neighborhoodChannels) > 0 {
		(*neighborhoodChannels)[NeighborToSendTo] <- *heartbeat
	}
}

//TODO: Theo
/* Adds updated heartbeat if does not exist in table, updates otherwise.
 *
 */
func updateHeartbeatTable(table *HeartbeatTable, update Heartbeat, quitChannel chan bool) {
	for _, entry := range table.heartbeats {
		if update.nodeId == entry.nodeId {
			if update.counter > entry.counter {
				entry.counter = update.counter
			} else if update.counter == entry.counter && entry.troubleCounter < 3 {
				entry.troubleCounter++
			} else {
				quitChannel <- true
			}
		}
	}
}
