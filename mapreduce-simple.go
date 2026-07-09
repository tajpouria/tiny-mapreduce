package main

import (
	"cmp"
	"fmt"
	"io"
	"log"
	"os"
	"slices"
	"tiny-mapreduce/utils"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintf(os.Stderr, "usage: mapreduce-simple <functionsplugin>.so <inputfile1> [inputfile]...")
		os.Exit(1)
	}

	mapFunc, reduceFunc := utils.LoadFuncsPlugin(os.Args[1])

	var intermediateKeyVal []utils.KeyVal
	for _, filename := range os.Args[2:] {
		inFile, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open in the input file %v %v", filename, err)
		}
		inContent, err := io.ReadAll(inFile)
		if err != nil {
			log.Fatalf("cannot read the content of the input file %v %v", filename, err)
		}
		inFile.Close()
		intermediateKeyVal = append(
			intermediateKeyVal,
			mapFunc(filename, string(inContent))...,
		)
	}

	slices.SortFunc(intermediateKeyVal, func(a, b utils.KeyVal) int {
		return cmp.Compare(a.Key, b.Key)
	})

	outfile, err := os.Create("wordcounter-out")
	if err != nil {
		log.Fatalf("cannot create output file %v", err)
	}
	defer outfile.Close()

	i := 0
	for i < len(intermediateKeyVal) {
		j := i + 1
		for j < len(intermediateKeyVal) {
			if intermediateKeyVal[i].Key == intermediateKeyVal[j].Key {
				j++
			} else {
				break
			}
		}
		var vals []string
		for k := i; k < j; k++ {
			vals = append(vals, intermediateKeyVal[k].Val)
		}
		reduceOut := reduceFunc(intermediateKeyVal[i].Key, vals)

		fmt.Fprintf(outfile, "%v %v\n",
			intermediateKeyVal[i].Key, reduceOut,
		)

		i = j
	}
}
