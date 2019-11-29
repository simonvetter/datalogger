package main

import (
	"fmt"
	"flag"
	"os"
	"time"
)

func main() {
	var confPath	string
	var conf	*Configuration
	var err		error
	var ticker	*time.Ticker
	var poller	*Poller
	var pollers	[]*Poller
	var sink	Sink
	var sinks	[]Sink
	var points	[]*Point

	flag.StringVar(&confPath, "conf", "path to configuration file", "")
	flag.Parse()

	if confPath == "" {
		fmt.Println("no configuration file given, aborting.")
		os.Exit(1)
	}

	conf, err	= Load(confPath)
	if err != nil {
		fmt.Printf("failed to load configuration file: %v\n", err)
		os.Exit(1)
	}

	// create and configure data sinks
	for idx, sc := range conf.Sinks {
		switch sc.Type {
		case "json":
			sink, err = NewFileSink(sc.Url, FILE_TYPE_JSON, sc.FifoSize,
					time.Duration(sc.MaxAge_ms) * time.Millisecond)

		case "csv":
			sink, err = NewFileSink(sc.Url, FILE_TYPE_CSV, sc.FifoSize,
					time.Duration(sc.MaxAge_ms) * time.Millisecond)

		case "influxdb":
			sink, err = NewInfluxDBSink(sc.Url, sc.FifoSize, 0,
					time.Duration(sc.MaxAge_ms) * time.Millisecond)

		case "timescaledb":
			sink, err = NewTimescaleDBSink(sc.Url, sc.Table, sc.FifoSize, 0,
					time.Duration(sc.MaxAge_ms) * time.Millisecond)

		case "console":
			sink, err = NewConsoleSink(sc.FifoSize)

		default:
			fmt.Printf("unsupported sink type '%v'\n", sc.Type)
			os.Exit(2)
		}

		if err != nil {
			fmt.Printf("failed to create sink #%v, skipping it: %v\n",
				   idx, err)
			continue
		}

		sinks	= append(sinks, sink)
	}

	if len(sinks) == 0 {
		fmt.Printf("no active sink, bailing out\n")
		os.Exit(2)
	}

	// create and configure pollers
	for idx := range conf.Pollers {
		poller, err	= NewPoller(conf.Pollers[idx])
		if err != nil {
			fmt.Printf("failed to create poller #%v, skipping it: %v\n",
				   idx, err)
			continue
		}

		pollers		= append(pollers, poller)
	}

	if len(pollers) == 0 {
		fmt.Printf("no active poller, bailing out\n")
		os.Exit(2)
	}

	ticker		= time.NewTicker(conf.DispatchRate)

	fmt.Printf("started modbus datalogger with %v pollers and %v sinks\n",
		   len(pollers), len(sinks))

	for {
		<-ticker.C

		// collect readings from all pollers
		for _, poller := range pollers {
			points	= append(points, poller.Points()...)
		}

		if len(points) <= 0 {
			continue
		}

		// pass on the collected values to all sinks
		for _, sink := range sinks {
			sink.Save(points)
		}

		points	= points[:0]
	}

	return
}
