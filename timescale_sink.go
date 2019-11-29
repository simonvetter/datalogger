package main

import (
	"database/sql"
	"fmt"
	"sync"
	"time"

	_ "github.com/lib/pq"
)

/*
* The TimescaleDB sink is meant to be used with Postgres/Timescale
* databases.
*
* The following statement can be used to create the destination table:
* CREATE TABLE metrics (
*	time TIMESTAMPTZ NOT NULL,
*	label TEXT NOT NULL,
*	value DOUBLE PRECISION NOT NULL);
*
* and (optionally), to convert metrics to a timescaleDB hypertable:
* SELECT create_hypertable('metrics', 'time');
*/

// TimescaleDB sink object
type TimescaleDBSink struct {
	fifo		[]*Point
	lock		sync.Mutex
	pushInterval	time.Duration
	url		string
	batchSize	uint
	bufferSize	uint
	table		string
	dbClient	*sql.DB
}

// Returns a new TimescaleDB sink.
func NewTimescaleDBSink(url string, table string, bufferSize uint, batchSize uint,
			pushInterval time.Duration) (ts *TimescaleDBSink, err error) {

	// buffer 100k points by default
	if bufferSize == 0 {
		bufferSize = 100000
	}

	// default to pushing 1k points at once
	if batchSize == 0 {
		batchSize  = 1000
	}

	ts = &TimescaleDBSink{
		fifo:		make([]*Point, 0),
		batchSize:	batchSize,
		bufferSize:	bufferSize,
		pushInterval:	pushInterval,
		url:		url,
		table:		table,
	}

	go ts.writer()

	return
}

// Pushes points to the sink's internal buffer. Points are rejected when
// the buffer is full.
func (ts *TimescaleDBSink) Save(points []*Point) (acceptedCount uint) {
	var pointCount uint

	pointCount	= uint(len(points))
	if pointCount == 0 {
		return
	}

	ts.lock.Lock()

	acceptedCount	= ts.bufferSize - uint(len(ts.fifo))

	if acceptedCount > pointCount {
		acceptedCount = pointCount
	}

	if acceptedCount > 0 {
		ts.fifo	= append(ts.fifo, points[0:acceptedCount]...)
	}
	ts.lock.Unlock()

	if acceptedCount != pointCount {
		fmt.Printf("timescaledb sink: dropped %v points out of %v\n",
			   pointCount - acceptedCount, pointCount)
	}

	return
}

// Periodically reads points from the internal buffer and pushes them
// to the database.
func (ts *TimescaleDBSink) writer() {
	var batch	[]*Point
	var batchSize	uint
	var ticker	*time.Ticker
	var err		error
	var dbClient	*sql.DB
	var tx		*sql.Tx
	var stmt	*sql.Stmt

	ticker	= time.NewTicker(ts.pushInterval)

	for {
		<-ticker.C

		ts.lock.Lock()
		// determine how many points to grab
		batchSize = uint(len(ts.fifo))
		if batchSize > ts.batchSize {
			batchSize = ts.batchSize
		}

		// grab batchSize points
		batch	= ts.fifo[0:batchSize]
		ts.lock.Unlock()

		if batchSize == 0 {
			continue
		}

		// get the database connection
		dbClient	= ts.getDBClient()
		if dbClient == nil {
			continue
		}

		// open a transaction
		tx, err	= dbClient.Begin()
		if err != nil {
			fmt.Printf("timescaledb sink: failed to create transaction: %v\n", err)
			continue
		}

		// prepare the query
		stmt, err	= tx.Prepare(
			fmt.Sprintf("INSERT INTO %s(time, label, value) VALUES ($1, $2, $3);",
				ts.table))

		if err != nil {
			fmt.Printf("timescaledb sink: failed to prepare statement: %v\n", err)
			tx.Rollback()
			continue
		}

		defer stmt.Close()

		// loop through the batch to insert every item
		for _, point := range batch {
			_,err		= stmt.Exec(point.Timestamp, point.Label, point.Value)
			if err != nil {
				fmt.Printf("timescaledb sink: failed to insert datapoint: %v\n", err)
				tx.Rollback()
				continue
			}
		}

		// commit the transaction
		err	= tx.Commit()
		if err != nil {
			fmt.Printf("timescaledb sink: failed to commit transaction: %v\n", err)
			continue
		}

		// free points which have been successfully pushed
		ts.lock.Lock()
		ts.fifo		= ts.fifo[batchSize:]
		ts.lock.Unlock()
	}

	// never reached
	return
}

func (ts *TimescaleDBSink) prepareDB() (err error) {
	ts.dbClient, err = sql.Open("postgres", ts.url)
	if err != nil {
		fmt.Printf("timescaledb sink: failed to connect: %v\n", err)

		ts.dbClient	= nil
		return
	}

	return
}

func (ts *TimescaleDBSink) getDBClient() (client *sql.DB) {
	var err	error

	if ts.dbClient == nil {
		err = ts.prepareDB()

		if err != nil {
			fmt.Printf("timescaledb sink: failed to connect: %v\n", err)
			return
		}
	}

	err	= ts.dbClient.Ping()
	if err != nil {
		fmt.Printf("timescaledb sink: failed to ping database: %v\n", err)
		return
	}

	client	= ts.dbClient

	return
}
