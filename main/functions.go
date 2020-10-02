package main

import (
	"strings"
	"unicode"
	"os"
	"sync"
	"sort"
	"fmt"
)

/*
 * Functions are added here to avoid using plugins. Plugins
 * are not currently supported on windows.
 */

func distributedMap(contents string) []KeyValue {

	// function to detect word separators.
	ff := func(r rune) bool { return !unicode.IsLetter(r) }

	// split contents into an array of words.
	words := strings.FieldsFunc(contents, ff)

	kva := []KeyValue{}
	for _, w := range words {
		kv := KeyValue{w, "1"}
		kva = append(kva, kv)
	}
	return kva
}


/* Calls given reduceFunction on given intermediate key-value pairs
 * Outputs is saved to given file path.
 */
 func distributedReduce(
	intermediatePairs []KeyValue,
	reduceJob func(jobId int,
		key string,
		values []string,
		responseChannel chan KeyValue,
		wg *sync.WaitGroup),
	outputFilePath string,
	reduceChannel chan KeyValue,
	wg *sync.WaitGroup) {

	outputFile, _ := os.Create(outputFilePath)
	output := make([]KeyValue, 0)

	i := 0
	jobCount := 0
	for i < len(intermediatePairs) {
		j := i + 1
		for j < len(intermediatePairs) && intermediatePairs[j].Key == intermediatePairs[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediatePairs[k].Value)
		}
		wg.Add(1)
		go reduceJob(jobCount, intermediatePairs[i].Key, values, reduceChannel, wg)
		jobCount++
		i = j
	}
	
	go func(wg *sync.WaitGroup, reduceChannel chan KeyValue) {
		wg.Wait()
		close(reduceChannel)
	}(wg, reduceChannel)

	for kv := range reduceChannel {
		output = append(output, kv)
	}
	
	//sort output
	sort.Sort(ByKey(output))

	//write to output file
	for _, o := range output {
		fmt.Fprintf(outputFile, "%v %v\n", o.Key, o.Value)
	}

	outputFile.Close()
}