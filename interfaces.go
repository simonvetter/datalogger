package main

import (
	"time"
)

type Point struct {
	Timestamp	time.Time
	Label		string
	Value		interface{}
}

type Sink interface {
	Save([]*Point)	(uint)
}
