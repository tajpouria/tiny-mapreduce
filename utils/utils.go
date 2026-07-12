package utils

import (
	"log"
	"plugin"
)

type KeyVal struct {
	Key string
	Val string
}

func LoadFuncsPlugin(filename string) (
	func(string, string) []KeyVal,
	func(string, []string) string,
) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot open func plugin %v %v", filename, err)
	}

	xMap, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find the func Map %v", err)
	}
	mapFunc := xMap.(func(string, string) []KeyVal)

	xReduce, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find the func Reduce %v", err)
	}
	reduceFunc := xReduce.(func(string, []string) string)

	return mapFunc, reduceFunc
}
