package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
	"strings"
)

func TestInfluxDBSinkFifo(t *testing.T) {
	var is		*InfluxDBSink
	var err		error
	var accepted	uint

	is, err	= NewInfluxDBSink("http://localhost?db=test", 10, 5, time.Second)
	if err != nil {
		t.Errorf("sink creation should have succeeded, got: %v", err)
	}

	if len(is.fifo) != 0 {
		t.Errorf("len(is.fifo) should have been 0, got: %v", len(is.fifo))
	}

	// save a few points until the fifo is full
	accepted	= is.Save([]*Point{
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
	if accepted != 4 {
		t.Errorf("all 4 points should have been accepted, actual: %v", accepted)
	}

	accepted	= is.Save([]*Point{
		{
			Timestamp:	time.Unix(1569150730, 0),
			Label:		"sensor0.temperature",
			Value:		18.8,
		},
		{
			Timestamp:	time.Unix(1569150730, 0),
			Label:		"sensor0.humidity",
			Value:		54.1,
		},
		{
			Timestamp:	time.Unix(1569150730, 1000000),
			Label:		"sensor1.temperature",
			Value:		-17.0,
		},
		{
			Timestamp:	time.Unix(1569150730, 2000000),
			Label:		"sensor1.humidity",
			Value:		60.19,
		},
		{
			Timestamp:	time.Unix(1569150732, 0),
			Label:		"sensor2.temperature",
			Value:		-17.0,
		},
		{
			Timestamp:	time.Unix(1569150732, 0),
			Label:		"sensor2.humidity",
			Value:		60.19,
		},
		{
			Timestamp:	time.Unix(1569150733, 0),
			Label:		"sensor2.humidity",
			Value:		60.19,
		},
	})
	if accepted != 6 {
		t.Errorf("6 points should have been accepted, saw: %v", accepted)
	}

	if len(is.fifo) != 10 {
		t.Errorf("expected 10 buffered points, saw: %v", len(is.fifo))
	}

	return
}

func TestInfluxDBSinkSerialize(t *testing.T) {
	var points	[]*Point
	var buf		bytes.Buffer
	var is		*InfluxDBSink
	var err		error
	var count	int

	is, err	= NewInfluxDBSink("http://localhost?db=test", 10, 5, time.Second)
	if err != nil {
		t.Errorf("sink creation should have succeeded, got: %v", err)
	}

	// out of all these points, only those with the following labels should
	// go through:
	// "house.kitchen.sensor2.humidity"
	// "house.sensor2.humidity",
	// "house.sensor2,altitude=14.humidity",
	points = []*Point{
		{
			Timestamp:	time.Unix(1569150732, 15000000),
			Label:		"house.kitchen.sensor2.humidity",
			Value:		60.19,
		},
		{
			Timestamp:	time.Unix(1569150733, 0),
			Label:		"humidity",
			Value:		60.19,
		},
		{
			Timestamp:	time.Unix(1569150733, 0),
			Label:		"humidity.",
			Value:		60.19,
		},
		{
			Timestamp:	time.Unix(1569150733, 0),
			Label:		"",
			Value:		60.19,
		},
		{
			Timestamp:	time.Unix(1569150733, 0),
			Label:		".",
			Value:		60.19,
		},
		{
			Timestamp:	time.Unix(1569150733, 0),
			Label:		"house.sensor2.humidity",
			Value:		60.19,
		},
		{
			Timestamp:	time.Unix(1569150733, 0),
			Label:		"house.sensor2,altitude=14.humidity",
			Value:		60.19,
		},
	}

	is.serialize(&buf, points)
	for idx, line := range strings.Split(buf.String(), "\n") {
		switch idx {
		case 0:
			if line != "house.kitchen.sensor2 humidity=60.19 1569150732015" {
				t.Errorf("unexpected output line at index %d: '%s'", idx, line)
			}

		case 1:
			if line != "house.sensor2 humidity=60.19 1569150733000" {
				t.Errorf("unexpected output line at index %d: '%s'", idx, line)
			}

		case 2:
			if line != "house.sensor2,altitude=14 humidity=60.19 1569150733000" {
				t.Errorf("unexpected output line at index %d: '%s'", idx, line)
			}

		case 3:
			if line != "" {
				t.Errorf("expected an empty string after the final carriage " +
					 "return, saw: '%s'", line)
			}

		default:
			t.Errorf("unexpected extra line: '%s'", line)
		}

		count++
	}

	if count != 4 {
		t.Errorf("expected 4 lines, got %v", count)
	}

	return
}

func TestInfluxDBSinkLogic(t *testing.T) {
	var is			*InfluxDBSink
	var err			error
	var accepted		uint
	var ts			*httptest.Server
	var requestCount	uint
	var payload		[]byte
	var expectedPayloads	[]string

	expectedPayloads	= []string{
		"sensor0 temperature=18.7 1569150729000\n",
		"sensor0 temperature=18.7 1569150729000\n",
		"sensor1 humitidy=93.1 1569150729000\n" +
			"sensor0 humitidy=10 1569150731000\n",
		"sensor0 speed_kph=597 1569150729000\n" +
		        "sensor1 speed_kph=598 1569150729000\n",
		"sensor0 speed_kph=597 1569150729000\n" +
		        "sensor1 speed_kph=598 1569150729000\n",
		"sensor2 speed_kph=597.1 1569150729000\n",
	}

	ts	= httptest.NewServer(
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				// make sure the URL is what we expect
				if r.URL.String() != "/write?db=testdb&precision=ms" {
					t.Errorf("unexpected request URL: %s", r.URL)
				}

				// read the entire body
				payload, err  = ioutil.ReadAll(r.Body)
				if err != nil {
					t.Errorf("failed to read request body: %v", err)
				}

				// compare payloads if the number of requests is in check
				if requestCount < uint(len(expectedPayloads)) &&
				   string(payload) != expectedPayloads[requestCount] {
					   t.Errorf("unexpected payload #%d: '%s'",
						    requestCount, string(payload))
				}

				switch(requestCount) {
				case 0:
					// reply with an error
					w.WriteHeader(400)

				case 1:
					// reply with 204 no content (OK)
					w.WriteHeader(204)

				case 2:
					// reply with 204 no content (OK)
					w.WriteHeader(204)

				case 3:
					// reply with 201 created (KO)
					w.WriteHeader(201)

				case 4:
					// reply with 204 no content (OK)
					w.WriteHeader(204)

				case 5,6:
					// reply with 204 created (OK)
					w.WriteHeader(204)

				default:
					w.WriteHeader(500)
				}

				requestCount++
			}))

	// close the test server once done
	defer ts.Close()

	is, err	= NewInfluxDBSink(fmt.Sprintf("%s/write?db=testdb", ts.URL),
				  10, 2, 100 * time.Millisecond)
	if err != nil {
		t.Errorf("sink creation should have succeeded, got: %v", err)
	}

	if len(is.fifo) != 0 {
		t.Errorf("len(is.fifo) should have been 0, got: %v", len(is.fifo))
	}

	// save one point
	accepted	= is.Save([]*Point{
		{
			Timestamp:	time.Unix(1569150729, 0),
			Label:		"sensor0.temperature",
			Value:		18.7,
		},
	})
	if accepted != 1 {
		t.Errorf("1 point should have been accepted, saw: %v", accepted)
	}

	time.Sleep(300 * time.Millisecond)

	// save two other points
	accepted	= is.Save([]*Point{
		{
			Timestamp:	time.Unix(1569150729, 0),
			Label:		"sensor1.humitidy",
			Value:		93.1,
		},
		{
			Timestamp:	time.Unix(1569150731, 0),
			Label:		"sensor0.humitidy",
			Value:		10,
		},
	})
	if accepted != 2 {
		t.Errorf("2 points should have been accepted, saw: %v", accepted)
	}

	time.Sleep(500 * time.Millisecond)


	// save 3 points
	accepted	= is.Save([]*Point{
		{
			Timestamp:	time.Unix(1569150729, 0),
			Label:		"sensor0.speed_kph",
			Value:		597,
		},
		{
			Timestamp:	time.Unix(1569150729, 0),
			Label:		"sensor1.speed_kph",
			Value:		598,
		},
		{
			Timestamp:	time.Unix(1569150729, 0),
			Label:		"sensor2.speed_kph",
			Value:		597.1,
		},
	})
	if accepted != 3 {
		t.Errorf("1 point should have been accepted, saw: %v", accepted)
	}

	time.Sleep(1 * time.Second)
	if requestCount != 6 {
		t.Errorf("expected 6 requests, saw: %v", requestCount)
	}

	return
}
