package main

import (
	"strconv"
	"strings"
	"tiny-mapreduce/utils"
	"unicode"
)

func Map(filename string, content string) []utils.KeyVal {
	words := strings.FieldsFunc(
		content,
		func(r rune) bool {
			return !unicode.IsLetter(r)
		},
	)
	var KeyValRes []utils.KeyVal
	for _, w := range words {
		KeyValRes = append(KeyValRes,
			utils.KeyVal{
				Key: w,
				Val: "1",
			},
		)
	}
	return KeyValRes
}

func Reduce(key string, values []string) string {
	return strconv.Itoa(len(values))
}
