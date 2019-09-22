package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"strings"
	"time"
)

func TestFileSink(t *testing.T) {
	var err		error
	var fs		*FileSink

	// pass an unknown file type, should fail
	fs, err	= NewFileSink("/tmp", 5, 100, time.Minute)
	if err == nil {
		t.Errorf("NewFileSink() should have failed")
	}

	// pass a non-existent path, should fail
	fs, err	= NewFileSink("/tmp33222", FILE_TYPE_CSV, 100, time.Minute)
	if err == nil {
		t.Errorf("NewFileSink() should have failed")
	}
	if err != nil && !os.IsNotExist(err) {
		t.Errorf("expected a file not found error, got: %v", err)
	}

	// pass an existent, non-directory path, should fail
	fs, err	= NewFileSink("/etc/hosts", FILE_TYPE_CSV, 100, time.Minute)
	if err == nil {
		t.Errorf("NewFileSink() should have failed")
	}

	// pass an existent directory as path
	fs, err	= NewFileSink("/tmp", FILE_TYPE_CSV, 100, time.Minute)
	if err != nil {
		t.Errorf("NewFileSink() should have succeeded, got: %v", err)
	}

	if len(fs.fifo) != 0 {
		t.Errorf("len(fs.fifo) should have been 0, got: %v", len(fs.fifo))
	}

	if cap(fs.fifo) != 100 {
		t.Errorf("cap(fs.fifo) should have been 100, got: %v", cap(fs.fifo))
	}

	if fs.maxAge != 1 * time.Minute {
		t.Errorf("expected fs.maxAge to be 1 * time.Minute, saw: %v", fs.maxAge)
	}

	return
}

func TestJsonFileSink(t *testing.T) {
	var err		error
	var fs		*FileSink
	var file	*os.File
	var contents	[]byte
	var path	string
	var entryCount	uint
	var accepted	uint

	// expected file path
	path	= fmt.Sprintf("/tmp/%s.json", time.Now().UTC().Format("2006-01-02"))

	// remvoe the test file to cleanup after ourselves
	defer os.Remove(path)

	// pass an existent directory as path
	fs, err	= NewFileSink("/tmp", FILE_TYPE_JSON, 2, 100 * time.Millisecond)
	if err != nil {
		t.Errorf("NewFileSink() should have succeeded, got: %v", err)
	}

	if cap(fs.fifo) != 2 {
		t.Errorf("cap(fs.fifo) should have been 2, got: %v", cap(fs.fifo))
	}

	// save a few points until the fifo is full
	accepted	= fs.Save([]*Point{
		{
			Timestamp:	time.Unix(1569150729, 0),
			Label:		"sensor0.temperature",
			Value:		18.7,
		},
		{
			Timestamp:	time.Unix(1569150729, 0),
			Label:		"sensor0.humidity",
			Value:		54,
		},
		{
			Timestamp:	time.Unix(1569150729, 1000000),
			Label:		"sensor1.temperature",
			Value:		-17.1,
		},
		{
			Timestamp:	time.Unix(1569150729, 2000000),
			Label:		"sensor1.humidity",
			Value:		60,
		},
	})
	if accepted != 2 {
		t.Errorf("only 2 points should have been accepted, actual: %v", accepted)
	}

	// give enough time for the writer goroutine to flush to disk
	time.Sleep(200 * time.Millisecond)
	if len(fs.fifo) != 0 {
		t.Errorf("fs.fifo should be empty, saw: %v", len(fs.fifo))
	}

	// attempt to open the file
	file, err	= os.Open(path)
	if err != nil {
		t.Errorf("%s should have existed, got: %v", path, err)
	}

	contents, err	= ioutil.ReadAll(file)
	if err != nil {
		t.Errorf("failed to read file: %v", err)
	}
	file.Close()

	for idx, line := range strings.Split(string(contents), "\n") {
		switch idx {
		case 0:
			if line != "{\"timestamp\":1569150729000,\"label\":\"sensor0.temperature\",\"value\":18.7}" {
				t.Errorf("unexpected line '%s'", line)
			}

		case 1:
			if line != "{\"timestamp\":1569150729000,\"label\":\"sensor0.humidity\",\"value\":54}" {
				t.Errorf("unexpected line '%s'", line)
			}

		case 2:
			if line != "" {
				t.Errorf("unexpected line '%s'", line)
			}

		default:
			t.Errorf("extra line #%d: '%s'", idx, line)
		}

		entryCount++
	}

	// counting the final empty token (after the last \n), we should have read 3 lines
	if entryCount != 3 {
		t.Errorf("expected 3 lines, saw: %v", entryCount)
	}

	// add another point
	accepted	= fs.Save([]*Point{
		{
			Timestamp:	time.Unix(1569150729, 2000000),
			Label:		"sensor1.humidity",
			Value:		60,
		},
	})
	if accepted != 1 {
		t.Errorf("1 point should have been accepted, actual: %v", accepted)
	}

	// give enough time for the writer goroutine to flush to disk
	time.Sleep(200 * time.Millisecond)
	if len(fs.fifo) != 0 {
		t.Errorf("fs.fifo should be empty, saw: %v", len(fs.fifo))
	}

	// attempt to re-open the file
	file, err	= os.Open(path)
	if err != nil {
		t.Errorf("%s should have existed, got: %v", path, err)
	}

	contents, err	= ioutil.ReadAll(file)
	if err != nil {
		t.Errorf("failed to read file: %v", err)
	}
	file.Close()

	entryCount	= 0
	for idx, line := range strings.Split(string(contents), "\n") {
		switch idx {
		case 0:
			if line != "{\"timestamp\":1569150729000,\"label\":\"sensor0.temperature\",\"value\":18.7}" {
				t.Errorf("unexpected line '%s'", line)
			}

		case 1:
			if line != "{\"timestamp\":1569150729000,\"label\":\"sensor0.humidity\",\"value\":54}" {
				t.Errorf("unexpected line '%s'", line)
			}

		case 2:
			if line != "{\"timestamp\":1569150729002,\"label\":\"sensor1.humidity\",\"value\":60}" {
				t.Errorf("unexpected line '%s'", line)
			}

		case 3:
			if line != "" {
				t.Errorf("unexpected line '%s'", line)
			}

		default:
			t.Errorf("extra line #%d: '%s'", idx, line)
		}

		entryCount++
	}

	if entryCount != 4 {
		t.Errorf("expected 4 lines, saw: %v", entryCount)
	}

	return
}

func TestCSVFileSink(t *testing.T) {
	var err		error
	var fs		*FileSink
	var file	*os.File
	var contents	[]byte
	var path	string
	var entryCount	uint
	var accepted	uint

	// expected file path
	path	= fmt.Sprintf("/tmp/%s.csv", time.Now().UTC().Format("2006-01-02"))

	// remvoe the test file to cleanup after ourselves
	defer os.Remove(path)

	// pass an existent directory as path
	fs, err	= NewFileSink("/tmp", FILE_TYPE_CSV, 2, 100 * time.Millisecond)
	if err != nil {
		t.Errorf("NewFileSink() should have succeeded, got: %v", err)
	}

	if cap(fs.fifo) != 2 {
		t.Errorf("cap(fs.fifo) should have been 2, got: %v", cap(fs.fifo))
	}

	// save a few points until the fifo is full
	accepted	= fs.Save([]*Point{
		{
			Timestamp:	time.Unix(1569150729, 0),
			Label:		"sensor0.temperature",
			Value:		18.7,
		},
		{
			Timestamp:	time.Unix(1569150729, 0),
			Label:		"sensor0.humidity",
			Value:		54,
		},
		{
			Timestamp:	time.Unix(1569150729, 1000000),
			Label:		"sensor1.temperature",
			Value:		-17.1,
		},
		{
			Timestamp:	time.Unix(1569150729, 2000000),
			Label:		"sensor1.humidity",
			Value:		60,
		},
	})
	if accepted != 2 {
		t.Errorf("only 2 points should have been accepted, actual: %v", accepted)
	}

	// give enough time for the writer goroutine to flush to disk
	time.Sleep(200 * time.Millisecond)
	if len(fs.fifo) != 0 {
		t.Errorf("fs.fifo should be empty, saw: %v", len(fs.fifo))
	}

	// attempt to open the file
	file, err	= os.Open(path)
	if err != nil {
		t.Errorf("%s should have existed, got: %v", path, err)
	}

	contents, err	= ioutil.ReadAll(file)
	if err != nil {
		t.Errorf("failed to read file: %v", err)
	}
	file.Close()

	for idx, line := range strings.Split(string(contents), "\n") {
		switch idx {
		case 0:
			if line != "1569150729000,sensor0.temperature,18.7" {
				t.Errorf("unexpected line '%s'", line)
			}

		case 1:
			if line != "1569150729000,sensor0.humidity,54" {
				t.Errorf("unexpected line '%s'", line)
			}

		case 2:
			if line != "" {
				t.Errorf("unexpected line '%s'", line)
			}

		default:
			t.Errorf("extra line #%d", idx)
		}

		entryCount++
	}

	if entryCount != 3 {
		t.Errorf("expected 3 lines, saw: %v", entryCount)
	}

	// add another point
	accepted	= fs.Save([]*Point{
		{
			Timestamp:	time.Unix(1569150729, 2000000),
			Label:		"sensor1.humidity",
			Value:		69,
		},
	})
	if accepted != 1 {
		t.Errorf("1 point should have been accepted, actual: %v", accepted)
	}

	// give enough time for the writer goroutine to flush to disk
	time.Sleep(200 * time.Millisecond)
	if len(fs.fifo) != 0 {
		t.Errorf("fs.fifo should be empty, saw: %v", len(fs.fifo))
	}

	// attempt to re-open the file
	file, err	= os.Open(path)
	if err != nil {
		t.Errorf("%s should have existed, got: %v", path, err)
	}

	contents, err	= ioutil.ReadAll(file)
	if err != nil {
		t.Errorf("failed to read file: %v", err)
	}
	file.Close()

	entryCount	= 0
	for idx, line := range strings.Split(string(contents), "\n") {
		switch idx {
		case 0:
			if line != "1569150729000,sensor0.temperature,18.7" {
				t.Errorf("unexpected line '%s'", line)
			}

		case 1:
			if line != "1569150729000,sensor0.humidity,54" {
				t.Errorf("unexpected line '%s'", line)
			}

		case 2:
			if line != "1569150729002,sensor1.humidity,69" {
				t.Errorf("unexpected line '%s'", line)
			}

		case 3:
			if line != "" {
				t.Errorf("unexpected line '%s'", line)
			}

		default:
			t.Errorf("extra line #%d: '%s'", idx, line)
		}

		entryCount++
	}

	if entryCount != 4 {
		t.Errorf("expected 4 lines, saw: %v", entryCount)
	}

	return
}
