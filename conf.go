package main

import (
	"errors"
	"io/ioutil"
	"encoding/json"
	"fmt"
	"time"

	"github.com/simonvetter/modbus"
)

type pollerConf struct {
	Url		string		`json:"url"`
	Targets		[]*targetConf	`json:"targets"`
	PollInterval_ms	uint		`json:"poll_interval_ms"`
	Timeout_ms	uint		`json:"timeout_ms"`
	Speed		uint		`json:"speed_bps"`
	DataBits	uint		`json:"data_bits"`
	StopBits	uint		`json:"stop_bits"`
	Parity		string		`json:"parity"`
	Endianness	string		`json:"endianness"`
	WordOrder	string		`json:"word_order"`
}

type targetConf struct {
	UnitId		uint8		`json:"unit_id"`
	RegType		string		`json:"register_type"`
	RegAddr		uint16		`json:"register_address"`
	Label		string		`json:"label"`
	ScaleFactor	float64		`json:"scale_factor"`
	Offset		float64		`json:"offset"`
	DecimalPlaces	uint		`json:"decimal_places"`
}

type sinkConf	struct {
	Type		string		`json:"type"`
	FifoSize	uint		`json:"fifo_size"`
	MaxAge_ms	uint		`json:"max_age_ms"`
	Url		string		`json:"url"`
}

type jsonConf struct {
	Pollers		[]*pollerConf	`json:"pollers"`
	Sinks		[]*sinkConf	`json:"sinks"`
	DispatchRate_ms uint		`json:"dispatch_rate_ms"`
}

// Main configuration object.
type Configuration struct {
	Pollers		[]*PollerConfiguration
	Sinks		[]*sinkConf
	DispatchRate	time.Duration
}

// Loads a JSON configuration file, validates it and builds out
// the necessary configuration objects.
func Load(path string) (conf *Configuration, err error) {
	var buf		[]byte
	var jsonConf	jsonConf
	var labels	map[string]bool

	labels		= make(map[string]bool)
	conf		= &Configuration{}

	// read the json file and unmarshal it
	buf, err	= ioutil.ReadFile(path)
	if err != nil {
		return
	}

	err		= json.Unmarshal(buf, &jsonConf)
	if err != nil {
		return
	}

	// dispatch_rate_ms is optional and defaults to 250ms
	conf.DispatchRate	= time.Duration(jsonConf.DispatchRate_ms) * time.Millisecond
	if conf.DispatchRate == 0 {
		conf.DispatchRate = 250 * time.Millisecond
	}

	if jsonConf.Pollers == nil {
		err	= errors.New("pollers section missing")
		return
	}

	if len(jsonConf.Pollers) == 0 {
		err	= errors.New("no poller defined")
		return
	}

	for _, pc := range jsonConf.Pollers {
		var pollerConf	PollerConfiguration

		switch pc.Parity {
		case "odd": pollerConf.Parity = modbus.PARITY_ODD
		case "even": pollerConf.Parity = modbus.PARITY_EVEN
		case "none", "": pollerConf.Parity = modbus.PARITY_NONE
		default:
			err = fmt.Errorf("unknown parity setting '%s'", pc.Parity)
			return
		}

		if pc.Url == "" {
			err = fmt.Errorf("poller url missing")
			return
		}
		pollerConf.Url		= pc.Url

		// timeout defaults to 1s
		pollerConf.Timeout	= time.Duration(pc.Timeout_ms) * time.Millisecond
		if pollerConf.Timeout == 0 {
			pollerConf.Timeout = 1 * time.Second
		}

		pollerConf.PollInterval = time.Duration(pc.PollInterval_ms) * time.Millisecond
		if pollerConf.PollInterval == 0 {
			err = fmt.Errorf("poller poll_interval_ms missing")
			return
		}

		pollerConf.Speed	= pc.Speed
		pollerConf.DataBits	= pc.DataBits
		pollerConf.StopBits	= pc.StopBits

		switch pc.Endianness {
		case "big", "bigendian", "":
			pollerConf.Endianness	= modbus.BIG_ENDIAN

		case "little", "littleendian":
			pollerConf.Endianness	= modbus.LITTLE_ENDIAN

		default:
			err = fmt.Errorf("unknown poller endianness setting '%s'",
					 pc.Endianness)
			return
		}

		switch pc.WordOrder {
		case "highfirst", "hf", "":
			pollerConf.WordOrder	= modbus.HIGH_WORD_FIRST

		case "lowfirst", "lf":
			pollerConf.WordOrder	= modbus.LOW_WORD_FIRST

		default:
			err = fmt.Errorf("unknown poller word order setting '%s'",
					 pc.WordOrder)
			return
		}

		for _, tc := range pc.Targets {
			var target *Target

			target = &Target{
				RegAddr:	tc.RegAddr,
				Label:		tc.Label,
				UnitId:		tc.UnitId,
				ScaleFactor:	tc.ScaleFactor,
				Offset:		tc.Offset,
				DecimalPlaces:	tc.DecimalPlaces,
			}

			// each target needs a system-wide unique label
			if target.Label == "" {
				err	= fmt.Errorf("missing target label")
				return
			}

			if labels[target.Label] {
				err	= fmt.Errorf("duplicate target label '%s'",
						     target.Label)
				return
			}

			switch tc.RegType {
			case "h:uint16", "holding:uint16":
				target.ValueType	= UINT16
				target.MbType		= modbus.HOLDING_REGISTER

			case "h:int16", "holding:int16":
				target.ValueType	= INT16
				target.MbType		= modbus.HOLDING_REGISTER

			case "h:uint32", "holding:uint32":
				target.ValueType	= UINT32
				target.MbType		= modbus.HOLDING_REGISTER

			case "h:int32", "holding:int32":
				target.ValueType	= INT32
				target.MbType		= modbus.HOLDING_REGISTER

			case "h:float32", "holding:float32":
				target.ValueType	= FLOAT32
				target.MbType		= modbus.HOLDING_REGISTER

			case "i:uint16", "input:uint16":
				target.ValueType	= UINT16
				target.MbType		= modbus.INPUT_REGISTER

			case "i:int16", "input:int16":
				target.ValueType	= INT16
				target.MbType		= modbus.INPUT_REGISTER

			case "i:uint32", "input:uint32":
				target.ValueType	= UINT32
				target.MbType		= modbus.INPUT_REGISTER

			case "i:int32", "input:int32":
				target.ValueType	= INT32
				target.MbType		= modbus.INPUT_REGISTER

			case "i:float32", "input:float32":
				target.ValueType	= FLOAT32
				target.MbType		= modbus.INPUT_REGISTER

			default:
				err	= fmt.Errorf("unknown register_type setting '%s'",
						     tc.RegType)
				return
			}

			pollerConf.Targets = append(pollerConf.Targets, target)

			// remember the target name
			labels[target.Label] = true
		}

		conf.Pollers = append(conf.Pollers, &pollerConf)
	}

	if jsonConf.Sinks == nil {
		err	= errors.New("sinks section missing")
		return
	}

	if len(jsonConf.Sinks) == 0 {
		err	= errors.New("no sinks defined")
		return
	}

	conf.Sinks	= jsonConf.Sinks

	return
}
