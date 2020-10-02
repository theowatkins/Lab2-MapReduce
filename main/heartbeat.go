package main

import (
	"sync"
	"time"
)

const TimeBetweenHeartbeats = 100 * time.Millisecond
const maxTrouble = 10

type Heartbeat struct {
	nodeId         string
	counter        int64
	troubleCounter int
}

type HeartbeatChannel chan []Heartbeat
type HeartbeatChannels []HeartbeatChannel
type HeartbeatChannelMap map[string]HeartbeatChannels
type NeighborAssignments map[string][]string

func runJobsWithHeartbeat(workUnits []WorkUnit) {
	numberOfJobs := len(workUnits)
	neighborsChannels := createNeighborhood(numberOfJobs)
	wg := new(sync.WaitGroup)
	killChannel := make(chan string)


	for workIndex, work := range workUnits {
		wg.Add(1)
		workIndex := workIndex
		work := work
		go func() {
			runJobWithHeartbeat(
				workIndex,
				neighborsChannels[generateNodeId(workIndex)],
				work.work,
				killChannel)
			wg.Done()
		}()
	}
	//TODO: check kill channel and redistribute work accordingly
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
func runJobWithHeartbeat(
	jobId int,
	neighborsChannel HeartbeatChannels,
	job func(),
	killChannel chan string) {

	jobWaitGroup := new(sync.WaitGroup)
	quitHeartbeatChannel := make(chan bool)

	//1. Begin heartbeat
	jobWaitGroup.Add(1)
	go func() {
		runHeartBeatThread(generateNodeId(jobId), neighborsChannel, quitHeartbeatChannel, killChannel)
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
const TicksTilSend = 5

func runHeartBeatThread(
	threadId string,
	neighborhoodChannels HeartbeatChannels,
	quitChannel chan bool,
	killChannel chan string) {

	threadWaitGroup := new(sync.WaitGroup)
	threadHeartbeat := Heartbeat{threadId, 0, 0}
	theadAggregateChannel := make(chan HeartbeatTable)
	theadIsAlive := true
	//TODO: make number of neighbors variable
	threadHeartbeatTable := []Heartbeat{threadHeartbeat, nil, nil, nil}
	
	//add thread to heartbeat table before its heart starts beating
	threadHeartbeatTable[0] = threadHeartbeat
	/*
	 * Heart beat thread - Increments counter and sends to neighbor when
	 */
	threadWaitGroup.Add(1)
	go func() {
		numTicks := 0
		for theadIsAlive {
			time.Sleep(TimeBetweenHeartbeats)
			if theadIsAlive { //function could have exited when sleeping, do NOT touch state
				heartbeatTick(&threadHeartbeat)
			}
			numTicks++
			//send table
			if numTicks == TicksTilSend {
				if len(neighborhoodChannels) > 0 {
					neighborhoodChannels[NeighborToSendTo] <- threadHeartbeatTable
				}
				numTicks = 0
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
				updateHeartbeatTable(&threadHeartbeatTable, heartbeatUpdate, threadId, quitChannel, killChannel)
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

func heartbeatTick(heartbeat *Heartbeat) {
	heartbeat.counter++
}


/* Updates heartbeat table and notifies of any dead nodes
 *
 */
func updateHeartbeatTable(table *HeartbeatTable, update HeartbeatTable, curId string, quitChannel chan bool, killChannel chan string) {
	//TODO: don't love 2 for loops but kind of necessary rn bc nodeId is a string and not the index in the table
	//so the tables aren't in the same order accross nodes
	for _, newHb := range update {
		//don't update your own heartbeat bc you're already handling that
		if newHb.nodeId != curId {
			for _, existingHb := range (*table) {
				if existingHb != nil && existingHb.nodeId == newHb.nodeId {
					//update entry
					if existingHb.counter < newHb.counter {
						//still beating
						existingHb.counter = newHb.counter
						existingHb.troubleCounter = 0
					}
					else if existingHb.counter >= newHb.counter && existingHb.troubleCounter < maxTrouble {
						//might be dead
						existingHb.troubleCounter++
					}
					else {
						//dead
						killChannel <- existingHb.nodeId
						//remove from table
						existingHb = nil
					}
				}
				//add entry if it doesn't exist already
				else if existingHb == nil {
					existingHb = newHb
				}
			}
		}
	}
}
