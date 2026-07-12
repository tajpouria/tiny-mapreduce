#!/usr/bin/env sh

export MAPREDUCE_M='40'
export MAPREDUCE_R='10'

plugin="${1:-./wordcounter-functions.so}"

go run mapreduce-worker.go "$plugin" &
w1=$!
go run mapreduce-worker.go "$plugin" &
w2=$!
go run mapreduce-worker.go "$plugin" &
w3=$!
go run mapreduce-master.go ./in/*.txt

kill "$w1" "$w2" "$w3" 2>/dev/null || true
