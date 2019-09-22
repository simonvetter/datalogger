package main

import (
	"bytes"
	"fmt"
	"net/http"
	"time"
	"strings"
	"sync"
)

// InfluxDB sink object.
type InfluxDBSink struct {
	fifo		[]*Point
	lock		sync.Mutex
	pushInterval	time.Duration
	url		string
	batchSize	uint
	bufferSize	uint
	client		*http.Client
}

// Returns a new influx db sink.
func NewInfluxDBSink(url string, bufferSize uint, batchSize uint,
		     pushInterval time.Duration) (is *InfluxDBSink, err error) {

	// buffer 100k points by default
	if bufferSize == 0 {
		bufferSize = 100000
	}

	// default to pushing 1k points at once
	if batchSize == 0 {
		batchSize  = 1000
	}

	is = &InfluxDBSink{
		fifo:		make([]*Point, 0),
		batchSize:	batchSize,
		bufferSize:	bufferSize,
		pushInterval:	pushInterval,
		url:		url,
		client:		&http.Client{
			Timeout:	1 * time.Minute,
		},
	}

	go is.writer()

	return
}

// Pushes points to the sink's internal buffer. Points are rejected when
// the buffer is full.
func (is *InfluxDBSink) Save(points []*Point) (acceptedCount uint) {
	var pointCount uint

	pointCount	= uint(len(points))
	if pointCount == 0 {
		return
	}

	is.lock.Lock()

	acceptedCount	= is.bufferSize - uint(len(is.fifo))

	if acceptedCount > pointCount {
		acceptedCount = pointCount
	}

	if acceptedCount > 0 {
		is.fifo	= append(is.fifo, points[0:acceptedCount]...)
	}
	is.lock.Unlock()

	if acceptedCount != pointCount {
		fmt.Printf("influxdb sink: dropped %v points out of %v\n",
			   pointCount - acceptedCount, pointCount)
	}

	return
}

// Periodically reads points from the internal buffer, serializes them and pushes
// them to influxdb API.
func (is *InfluxDBSink) writer() {
	var batch	[]*Point
	var batchSize	uint
	var ticker	*time.Ticker
	var buf		bytes.Buffer
	var res		*http.Response
	var err		error

	ticker	= time.NewTicker(is.pushInterval)

	for {
		// clear the send buffer
		buf.Reset()

		<-ticker.C

		is.lock.Lock()
		// determine how many points to grab
		batchSize = uint(len(is.fifo))
		if batchSize > is.batchSize {
			batchSize = is.batchSize
		}

		// grab batchSize points
		batch	= is.fifo[0:batchSize]
		is.lock.Unlock()

		if batchSize == 0 {
			continue
		}

		// serialize the batch
		is.serialize(&buf, batch)

		// post the serialized payload to influxdb
		res, err	= is.client.Post(
			fmt.Sprintf("%s&precision=ms", is.url), "text", &buf)
		if err != nil {
			fmt.Printf("failed to perform influxdb POST request: %v\n", err)
			continue
		}
		res.Body.Close()

		if res.StatusCode != http.StatusNoContent {
			fmt.Printf("influxdb POST request failed with status code: %d\n",
				   res.StatusCode)
			continue
		}

		// free points which have been successfully pushed
		is.lock.Lock()
		is.fifo		= is.fifo[batchSize:]
		is.lock.Unlock()
	}

	return
}

// Turns points into influxdb line protocol entries.
func (is *InfluxDBSink) serialize(buf *bytes.Buffer, points []*Point) {
	var idx		int
	var fieldName	string

	for _, p := range points {
		if p == nil {
			continue
		}

		// locate the last dot
		idx		= strings.LastIndex(p.Label, ".")

		// if there is no dot or if the dot is either at the beginning or at the end
		// of the label, we can't split on it: drop the point.
		if idx < 1 || idx == len(p.Label) - 1 {
			fmt.Printf("discarding point with label '%s': cannot split on '.'\n",
				   p.Label)
			continue
		}

		// use the last token as field name
		fieldName	= p.Label[idx+1:len(p.Label)]

		buf.WriteString(
			fmt.Sprintf("%s %s=%v %v\n",
				p.Label[0:idx], fieldName,
				p.Value, p.Timestamp.UnixNano() / 1e6))
	}

	return
}
