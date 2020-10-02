package main

import (
	"fmt"
	"sync"
	"time"
)

const TimeBetweenHeartbeats = time.Second
const maxTrouble = 10

type Heartbeat struct {
	nodeId         int
	counter        int64
	troubleCounter int
}

type HeartbeatChannel chan []Heartbeat
type HeartbeatChannels []HeartbeatChannel
type HeartbeatChannelMap map[int]HeartbeatChannels
type NeighborAssignments map[int][]int
type KillChannel chan int

func runJobsWithHeartbeat(workUnits []WorkUnit) {
	numberOfJobs := len(workUnits)
	neighborsChannels := createNeighborhood(numberOfJobs)
	wg := new(sync.WaitGroup)
	killChannel := make(KillChannel)

	for workIndex, work := range workUnits {
		wg.Add(1)
		workIndex := workIndex
		work := work
		go func() {
			runJobWithHeartbeat(
				workIndex,
				neighborsChannels[workIndex],
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
	killChannel KillChannel) {

	jobWaitGroup := new(sync.WaitGroup)
	quitHeartbeatChannel := make(chan bool)

	//1. Begin heartbeat
	jobWaitGroup.Add(1)
	go func() {
		runHeartBeatThread(jobId, neighborsChannel, quitHeartbeatChannel, killChannel)
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
	threadIndex int,
	neighborhoodChannels HeartbeatChannels,
	quitChannel chan bool,
	killChannel KillChannel) {

	threadWaitGroup := new(sync.WaitGroup)
	theadAggregateChannel := make(chan []Heartbeat)
	theadIsAlive := true
	threadHeartbeatTable := make([]Heartbeat, NeighborhoodSize)

	//add thread to heartbeat table before its heart starts beating
	//and mark spots for neighbors as not populated yet
	threadHeartbeatTable[0] = Heartbeat{threadIndex, 0, 0}
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
				threadHeartbeatTable[0].counter++
				numTicks++
				//send table
				if numTicks == TicksTilSend {
					neighborhoodChannels[NeighborToSendTo] <- threadHeartbeatTable
					numTicks = 0
				}
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
				updateHeartbeatTable(&threadHeartbeatTable, heartbeatUpdate, threadIndex, killChannel)
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
func updateHeartbeatTable(
	table *[]Heartbeat,
	update []Heartbeat,
	curId int,
	killChannel KillChannel,
) {
	// fmt.Println("Before: ", (*table))
	// fmt.Println("Update: ", update)
	// fmt.Println("Updating...")

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
						fmt.Println("NODE DIED AHHHHHHHHHHHHH")
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

	// fmt.Println("After: ", (*table))
	// fmt.Println()
}
