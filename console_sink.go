package main

import (
	"fmt"
)

type ConsoleSink struct {
	fifo	chan *Point
}

// Returns a new console sink.
func NewConsoleSink(fifoSize uint) (cs *ConsoleSink, err error) {
	if fifoSize == 0 {
		fifoSize = 300
	}

	cs = &ConsoleSink{
		fifo: make(chan *Point, fifoSize),
	}

	go cs.writer()

	return
}

// Buffers points for later disk write. Points are rejected when the fifo is full.
func (cs *ConsoleSink) Save(points []*Point) (acceptedCount uint) {
	for _, p := range points {
		if len(cs.fifo) < cap(cs.fifo) {
			cs.fifo <- p
			acceptedCount++
		}
	}

	return
}

func (cs *ConsoleSink) writer() {
	var p *Point

	for {
		p = <-cs.fifo

		// drop nil points
		if p == nil {
			continue
		}

		fmt.Printf("timestamp: %v, label: %s, value: %v\n",
			   p.Timestamp, p.Label, p.Value)
	}

	return
}
