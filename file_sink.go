package main

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"os"
	"time"
)

const (
	FILE_TYPE_CSV	uint	= 1
	FILE_TYPE_JSON	uint	= 2
)

type FileSink struct {
	path		string
	file		*os.File
	fileType	uint
	fifo		chan *Point
	maxAge		time.Duration
	lastSave	time.Time
	recordCount	uint
}

// Returns a new file sink.
func NewFileSink(path string, fileType uint, fifoSize uint, maxAge time.Duration) (fs *FileSink, err error) {
	var writeInterval	time.Duration
	var fileInfo		os.FileInfo

	fs = &FileSink{
		path:		path,
		fileType:	fileType,
		maxAge:		maxAge,
	}

	if fileType != FILE_TYPE_JSON && fileType != FILE_TYPE_CSV {
		err	= fmt.Errorf("unsupported file type %v", fileType)
		return
	}

	// make sure path is accessible and a directory
	fileInfo, err	= os.Stat(fs.path)
	if err != nil {
		fmt.Printf("failed to stat path '%v': %v'\n", fs.path, err)
		return
	}

	if !fileInfo.IsDir() {
		err	= fmt.Errorf("%s is not a directory", fs.path)
		return
	}

	// default to 1k points
	if fifoSize == 0 {
		fifoSize	= 1000
	}

	// default to writing once every minute
	if fs.maxAge == 0 {
		fs.maxAge	= 1 * time.Minute
	}

	if fs.maxAge < 1 * time.Second {
		writeInterval	= fs.maxAge
	} else {
		writeInterval	= 1 * time.Second
	}

	fs.fifo	= make(chan *Point, fifoSize)

	go fs.writer(writeInterval)

	return
}

// Buffers points for later disk write. Points are rejected when the fifo is full.
func (fs *FileSink) Save(points []*Point) (acceptedCount uint) {
	for _, p := range points {
		if len(fs.fifo) < cap(fs.fifo) {
			fs.fifo <- p
			acceptedCount++
		}
	}

	return
}

// Writer goroutine. Creates one datafile per day and invokes write() as appropriate.
func (fs *FileSink) writer(tickRate time.Duration) {
	var err			error
	var highWaterMark	int
	var ticker		*time.Ticker
	var file		*os.File
	var filePath		string
	var fileName		string
	var now			time.Time
	var dayOfMonth		int

	highWaterMark	= (cap(fs.fifo) * 80) / 100

	ticker		= time.NewTicker(tickRate)

	for {
		<-ticker.C

		now	= time.Now().UTC()

		// close the file on day changes
		if dayOfMonth != now.Day() {
			dayOfMonth = now.Day()

			if fs.file != nil {
				fs.file.Close()
				fs.file	= nil

				fmt.Printf("saved %d records in %s\n", fs.recordCount, filePath)
				fs.recordCount	= 0
			}
		}

		// open a log file if needed, in the form path/YYYY-MM-DD.suffix, with suffix
		// either csv or json, depending on the file type.
		if fs.file == nil {
			fileName	= now.Format("2006-01-02")

			if fs.fileType == FILE_TYPE_JSON {
				fileName += ".json"
			} else {
				fileName += ".csv"
			}

			filePath	= filepath.Join(fs.path, fileName)
			file, err	= os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
			// if we failed to open the file, sleep for 30s before trying again
			if err != nil {
				fmt.Printf("failed to open file '%s': %v\n", filePath, err)
				time.Sleep(30 * time.Second)
				continue
			}
			fs.file		= file

			fmt.Printf("opened %s for writing\n", filePath)
		}

		if time.Since(fs.lastSave) >= fs.maxAge || len(fs.fifo) > highWaterMark {
		   err	= fs.write()
		   if err != nil {
			   fmt.Printf("failed to write: %v\n", err)
			   continue
		   }

		   fs.lastSave	= time.Now()
		}
	}

	return
}

// Flushes the fifo contents to disk.
func (fs *FileSink) write() (err error) {
	var buf		bytes.Buffer
	var count	uint
	var p		*Point

	if fs.file == nil {
		err	= errors.New("sink closed")
		return
	}

	for len(fs.fifo) > 0 {
		p = <-fs.fifo

		// drop nil points
		if p == nil {
			continue
		}

		switch fs.fileType {
		case FILE_TYPE_CSV:
			_, err = buf.WriteString(
				fmt.Sprintf("%d,%s,%v\n",
					    p.Timestamp.UnixNano() / 1e6,
					    p.Label,
					    p.Value))

		case FILE_TYPE_JSON:
			_, err = buf.WriteString(
				fmt.Sprintf("{\"timestamp\":%d,\"label\":\"%s\",\"value\":%v}\n",
					    p.Timestamp.UnixNano() / 1e6,
					    p.Label,
					    p.Value))
		default:
			err	= fmt.Errorf("unknown file type %v", fs.fileType)
			return
		}

		// if we failed to serialize the point for whatever reason, drop it
		if err != nil {
			fmt.Printf("failed to serialize point (%v): %v\n", p, err)
			continue
		}
		count++
	}

	_,err	= buf.WriteTo(fs.file)
	buf.Reset()

	// keep track of successful writes
	if err == nil {
		fs.recordCount += count
	}

	return
}
