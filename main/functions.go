package main

import (
	"strconv"
	"strings"
	"unicode"
)

/*
 * Functions are added here to avoid using plugins. Plugins
 * are not currently supported on windows.
 */

type MapFunction = func(string) []KeyValue
type ReduceFunction = func(string, []string) string

func Map(contents string) []KeyValue {

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

func Reduce(key string, values []string) string {
	return strconv.Itoa(len(values))
}
