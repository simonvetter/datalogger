package main

import (
	"testing"
	"time"
)

func TestConsoleSink(t *testing.T) {
	var err		error
	var cs		*ConsoleSink
	var accepted	uint

	cs, err	= NewConsoleSink(3)
	if err != nil {
		t.Errorf("NewConsoleSink() should have succeeded, got: %v", err)
	}

	if len(cs.fifo) != 0 {
		t.Errorf("len(cs.fifo) should have been 0, got: %v", len(cs.fifo))
	}

	if cap(cs.fifo) != 3 {
		t.Errorf("cap(cs.fifo) should have been 3, got: %v", cap(cs.fifo))
	}

	accepted = cs.Save([]*Point{
		{
			Timestamp:	time.Now(),
			Label:		"reg1",
			Value:		0x112233,
		},
		{
			Timestamp:	time.Now(),
			Label:		"reg2",
			Value:		0x112233,
		},
		{
			Timestamp:	time.Now(),
			Label:		"reg3",
			Value:		0x112273,
		},
	})
	if accepted < 2 {
		t.Errorf("at least 2 points should have been accepted, saw: %v", accepted)
	}

	// give the worker some time to flush the fifo
	time.Sleep(10 * time.Millisecond)
	if len(cs.fifo) != 0 {
		t.Errorf("the fifo should be empty, saw %v values", len(cs.fifo))
	}

	// make sure that the writer goroutine is resilient against nil points
	accepted = cs.Save([]*Point{
		nil,
		nil,
	})

	// give the worker some time to flush the fifo
	time.Sleep(10 * time.Millisecond)
	if len(cs.fifo) != 0 {
		t.Errorf("the fifo should be empty, saw %v values", len(cs.fifo))
	}

	return
}
