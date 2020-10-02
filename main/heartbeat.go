package main

import (
	"fmt"
	"sync"
	"time"
)

const TimeBetweenHeartbeats = 10 * time.Millisecond
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

	channelMap := make(map[HeartbeatChannel]int)

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
const NeighborhoodSize = numberOfNeighbors + 1

func runHeartBeatThread(
	threadId string,
	neighborhoodChannels HeartbeatChannels,
	quitChannel chan bool,
	killChannel chan string) {

	threadWaitGroup := new(sync.WaitGroup)
	threadHeartbeat := Heartbeat{threadId, 0, 0}
	theadAggregateChannel := make(chan []Heartbeat)
	theadIsAlive := true
	threadHeartbeatTable := make([]Heartbeat, NeighborhoodSize)
	
	//add thread to heartbeat table before its heart starts beating
	//and mark spots for neighbors as not populated yet
	threadHeartbeatTable[0] = threadHeartbeat
	for i := 1; i < NeighborhoodSize; i++ {
		//counter of -1 means uninitialized in table
		threadHeartbeatTable[i].counter = -1
	}

	/*
	 * Heart beat thread - Increments counter and sends table to neighbors after a few heartbeats
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
func updateHeartbeatTable(table *[]Heartbeat, update []Heartbeat, curId string, quitChannel chan bool, killChannel chan string) {
	fmt.Println("Before: ", (*table))
	fmt.Println("Update: ", update)
	fmt.Println("Updating...")

	//TODO: don't love 2 for loops but kind of necessary rn bc nodeId is a string and not the index in the table
	//so the tables aren't in the same order accross nodes
	for _, newHb := range update {
		if newHb.counter != -1 && newHb.nodeId != curId {
			//skip first slot because that's your own heartbeat
			for i := 1; i < len(*table); i++ {
				if (*table)[i].counter != -1 && (*table)[i].nodeId == newHb.nodeId {
					//update entry
					if (*table)[i].counter < newHb.counter {
						//still beating
						(*table)[i].counter = newHb.counter
						(*table)[i].troubleCounter = 0
					} else if (*table)[i].counter >= newHb.counter && (*table)[i].troubleCounter < maxTrouble {
						//might be dead
						(*table)[i].troubleCounter++
					} else {
						//dead
						killChannel <- (*table)[i].nodeId
						//remove from table
						(*table)[i].counter = -1
					}
				} else if (*table)[i].counter == -1 {
					//add to table if entry doesn't already exist
					(*table)[i].nodeId = newHb.nodeId
					(*table)[i].counter = newHb.counter 
					(*table)[i].troubleCounter = newHb.troubleCounter
					break
				}
			}
		}
	}

	fmt.Println("After: ", (*table))
	fmt.Println()
}
