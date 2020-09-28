package main

import (
	"strings"
	"unicode"
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


