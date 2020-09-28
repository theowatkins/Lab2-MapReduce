package main

type KeyValue struct {
	Key string
	Value string
}

type NodeInfo struct {
	Id int
}

/*
 * Code common to sequential and distributed code
 */
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func createInitializedChannelList(nChannels int)[]chan []KeyValue {
	channels := make([]chan []KeyValue, nChannels)
	for channelIndex := 0; channelIndex < nChannels; channelIndex++ {
		channels[channelIndex] = make(chan []KeyValue)
	}
	return channels
}

func createInitializedReduceChannelList(nChannels int)[]chan string {
	channels := make([]chan string, nChannels)
	for channelIndex := 0; channelIndex < nChannels; channelIndex++ {
		channels[channelIndex] = make(chan string)
	}
	return channels
}
