package main

import (
	"testing"
	"os"
	"time"

	"github.com/simonvetter/modbus"
)

func TestLoadConf(t *testing.T) {
	var err		error
	var conf	*Configuration

	conf, err	= Load("./non/existent")
	if err == nil {
		t.Errorf("Load() should have failed")
	}
	if !os.IsNotExist(err) {
		t.Errorf("Load() should have returned file not found, got: %v", err)
	}

	conf, err	= Load("./etc/example-conf.json")
	if err != nil {
		t.Errorf("Load() should have succeeded")
	}
	if conf == nil {
		t.Errorf("Load() should have returned a non-nil object")
	}

	if conf.DispatchRate != 250 * time.Millisecond {
		t.Errorf("conf.DispatchRate should have been 250ms")
	}

	if len(conf.Pollers) != 2 {
		t.Errorf("expected 2 pollers, got: %v", len(conf.Pollers))
	}

	for idx, pc := range conf.Pollers {
		switch idx {
		case 0:
			if pc.Url != "tcp://plc:502" {
				t.Errorf("poller #%v: url should have been '%s', saw: '%s'",
					 idx, "tcp://plc:502", pc.Url)
			}
			if pc.Speed != 0 {
				t.Errorf("poller #%v: speed should have been 0, saw %v:",
					 idx, pc.Speed)
			}
			if pc.DataBits != 0 {
				t.Errorf("poller #%v: dataBits should have been 0, saw %v:",
					 idx, pc.DataBits)
			}
			if pc.StopBits != 0 {
				t.Errorf("poller #%v: stopBits should have been 0, saw %v:",
					 idx, pc.StopBits)
			}
			if pc.Endianness != modbus.BIG_ENDIAN {
				t.Errorf("poller #%v: endianness should have been %v, saw: %v",
					 idx, modbus.BIG_ENDIAN, pc.Endianness)
			}
			if pc.PollInterval != 5 * time.Second {
				t.Errorf("poller #%v: pollInterval should have been %v, saw: %v",
					 idx, 5 * time.Second, pc.PollInterval)
			}

			if len(pc.Targets) != 5 {
				t.Errorf("poller #%v: expected 5 targets, got: %v",
					 idx, len(pc.Targets))
			}

			for i, expected := range []*Target{
				{
					UnitId:		0,
					MbType:		modbus.HOLDING_REGISTER,
					ValueType:	UINT32,
					Label:		"plc.system_time",
					RegAddr:	0,
					ScaleFactor:	0,
					Offset:		0,
					DecimalPlaces:	0,
				},
				{
					UnitId:		0,
					MbType:		modbus.HOLDING_REGISTER,
					ValueType:	INT16,
					Label:		"living_room.sensor0.temperature_C",
					RegAddr:	258,
					ScaleFactor:	0.1,
					Offset:		0,
					DecimalPlaces:	1,
				},
				{
					UnitId:		0,
					MbType:		modbus.HOLDING_REGISTER,
					ValueType:	UINT16,
					Label:		"living_room.sensor0.relHumidity_pc",
					RegAddr:	259,
					ScaleFactor:	0.1,
					Offset:		0,
					DecimalPlaces:	1,
				},
				{
					UnitId:		0,
					MbType:		modbus.HOLDING_REGISTER,
					ValueType:	INT16,
					Label:		"living_room.sensor1.temperature_C",
					RegAddr:	274,
					ScaleFactor:	0.1,
					Offset:		0,
					DecimalPlaces:	1,
				},
				{
					UnitId:		0,
					MbType:		modbus.HOLDING_REGISTER,
					ValueType:	UINT16,
					Label:		"living_room.sensor1.relHumidity_pc",
					RegAddr:	275,
					ScaleFactor:	0.1,
					Offset:		10.5,
					DecimalPlaces:	1,
				},
			} {
				if !confTestTargetEqual(pc.Targets[i], expected) {
					t.Errorf("poller %v: unexpected target #%d: %+v",
						 idx, i, pc.Targets[i])
				}
			}

		case 1:
			if pc.Url != "rtu:///dev/ttyUSB0" {
				t.Errorf("poller #%v: url should have been '%s', saw: '%s'",
					 idx, "rtu:///dev/ttyUSB0", pc.Url)
			}
			if pc.Speed != 19200 {
				t.Errorf("poller #%v: speed should have been 0, saw %v:",
					 idx, pc.Speed)
			}
			if pc.DataBits != 0 {
				t.Errorf("poller #%v: dataBits should have been 0, saw %v:",
					 idx, pc.DataBits)
			}
			if pc.StopBits != 0 {
				t.Errorf("poller #%v: stopBits should have been 0, saw %v:",
					 idx, pc.StopBits)
			}
			if pc.Endianness != modbus.BIG_ENDIAN {
				t.Errorf("poller #%v: endianness should have been %v, saw: %v",
					 idx, modbus.BIG_ENDIAN, pc.Endianness)
			}
			if pc.PollInterval != 1 * time.Second {
				t.Errorf("poller #%v: pollInterval should have been %v, saw: %v",
					 idx, 1 * time.Second, pc.PollInterval)
			}

			if len(pc.Targets) != 4 {
				t.Errorf("poller #%v: expected 4 targets, got: %v",
					 idx, len(pc.Targets))
			}

			for i, expected := range []*Target{
				{
					UnitId:		5,
					MbType:		modbus.INPUT_REGISTER,
					ValueType:	UINT32,
					Label:		"main_power_meter.u12_V",
					RegAddr:	50514,
					ScaleFactor:	0.01,
					Offset:		0,
					DecimalPlaces:	3,
				},
				{
					UnitId:		5,
					MbType:		modbus.INPUT_REGISTER,
					ValueType:	UINT32,
					Label:		"main_power_meter.u23_V",
					RegAddr:	50516,
					ScaleFactor:	0.01,
					Offset:		0,
					DecimalPlaces:	3,
				},
				{
					UnitId:		5,
					MbType:		modbus.INPUT_REGISTER,
					ValueType:	UINT32,
					Label:		"main_power_meter.u31_V",
					RegAddr:	50518,
					ScaleFactor:	0.01,
					Offset:		0,
					DecimalPlaces:	3,
				},
				{
					UnitId:		5,
					MbType:		modbus.HOLDING_REGISTER,
					ValueType:	FLOAT32,
					Label:		"main_power_meter.p_kW",
					RegAddr:	30500,
					ScaleFactor:	0,
					Offset:		0,
					DecimalPlaces:	3,
				},
			} {
				if !confTestTargetEqual(pc.Targets[i], expected) {
					t.Errorf("poller %v: unexpected target #%d: %+v",
						 idx, i, pc.Targets[i])
				}
			}
		}
	}

	if len(conf.Sinks) != 4 {
		t.Errorf("expected 4 sinks, got: %v", len(conf.Sinks))
	}

	if conf.Sinks[0].Type != "json" {
		t.Errorf("unexpected type for sink #0: %v", conf.Sinks[0].Type)
	}
	if conf.Sinks[0].Url != "/mnt/data_archive/json" {
		t.Errorf("unexpected url for sink #0: %v", conf.Sinks[0].Url)
	}
	if conf.Sinks[0].MaxAge_ms != 30000 {
		t.Errorf("unexpected maxAge_ms for sink #0: %v", conf.Sinks[0].MaxAge_ms)
	}
	if conf.Sinks[0].FifoSize != 0 {
		t.Errorf("unexpected fifoSize for sink #0: %v", conf.Sinks[0].FifoSize)
	}

	if conf.Sinks[2].Type != "influxdb" {
		t.Errorf("unexpected type for sink #2: %v", conf.Sinks[2].Type)
	}
	if conf.Sinks[2].Url != "http://localhost:8086/write?db=house" {
		t.Errorf("unexpected url for sink #2: %v", conf.Sinks[2].Url)
	}
	if conf.Sinks[2].MaxAge_ms != 5000 {
		t.Errorf("unexpected maxAge_ms for sink #2: %v", conf.Sinks[2].MaxAge_ms)
	}
	if conf.Sinks[2].FifoSize != 200000 {
		t.Errorf("unexpected fifoSize for sink #2: %v", conf.Sinks[2].FifoSize)
	}

	return
}

func confTestTargetEqual(a *Target, b *Target) (yes bool) {
	if a == nil || b == nil {
		return
	}

	if a.UnitId != b.UnitId ||
	   a.MbType != b.MbType ||
	   a.ValueType != b.ValueType ||
	   a.Label != b.Label ||
	   a.RegAddr != b.RegAddr ||
	   a.ScaleFactor != b.ScaleFactor ||
	   a.Offset != b.Offset ||
	   a.DecimalPlaces != b.DecimalPlaces {
		   return
	 }

	 yes = true

	return
}
