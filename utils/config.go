package utils

import (
	"os"
	"strconv"
)

const (
	defaultM = 8
	defaultR = 4
)

func NumMapTask() int { return envInt("MAPREDUCE_M", defaultM) }

func NumReduceTask() int { return envInt("MAPREDUCE_R", defaultR) }

func envInt(key string, def int) int {
	s := os.Getenv(key)
	if s == "" {
		return def
	}
	if v, err := strconv.Atoi(s); err == nil && v > 0 {
		return v
	}
	return def
}
