package main

import (
	"fmt"
	"math"
	"os"
	"sync"
	"time"

	"github.com/simonvetter/modbus"
)

const (
	UINT16	uint	= 1
	INT16	uint	= 2
	UINT32	uint	= 3
	INT32	uint	= 4
	FLOAT32	uint	= 5
)

// The Target object describes a target data value.
type Target struct {
	MbType		modbus.RegType	// modbus register type:
					// either modbus.HOLDING_REGISTER
					// or modbus.INPUT_REGISTER
	ValueType	uint		// how to decode the value after reading it from
					// the modbus device (e.g. UINT16, FLOAT32, etc.)
	UnitId		uint8		// modbus device unit ID (slave ID)
	RegAddr		uint16		// base modbus register address
	Label		string		// (text) label describing the value
	ScaleFactor	float64		// scale factor applied to the value (disabled if 0)
	Offset		float64		// offset applied to the value (disabled if 0)
	DecimalPlaces	uint		// round the to x decimal places (disabled if 0)
}

type PollerConfiguration struct {
	Url		string		// modbus client target URL
					// (e.g. tcp://somehost:502 or rtu:///dev/ttyUSB0)
	Targets		[]*Target	// target values to query on this modbus link
	PollInterval	time.Duration	// how long to wait between target polls
	Timeout		time.Duration	// modbus request timeout parameter
	Endianness	modbus.Endianness // endianness of the modbus registers:
					  // either modbus.BIG_ENDIAN or modbus.LITTLE_ENDIAN
	WordOrder	modbus.WordOrder  // word order of modbus registers for 32-bit
					  // values: either modbus.HIGH_WORD_FIRST
					  // or modbus.LOW_WORD_FIRST

					// serial link parameters:
	Speed		uint		// baud rate
	DataBits	uint		// number of data bits per character
	StopBits	uint		// number of stop bits
	Parity		uint		// parity: modbus.PARITY_NONE, modbus.PARITY_ODD or
					// modbus.PARITY_EVEN
}

// Poller pbject.
type Poller struct {
	conf		*PollerConfiguration
	lock		sync.Mutex
	mc		*modbus.ModbusClient
	points		[]*Point
}

// Returns a new poller.
func NewPoller(conf *PollerConfiguration) (p *Poller, err error) {
	p = &Poller{
		conf:	conf,
	}

	p.mc, err = modbus.NewClient(&modbus.ClientConfiguration{
		URL:		conf.Url,
		Speed:		conf.Speed,
		DataBits:	conf.DataBits,
		Parity:		conf.Parity,
		StopBits:	conf.StopBits,
		Timeout:	conf.Timeout,
	})

	if err != nil {
		return
	}

	go p.poll()

	return
}

// Returns a slice containing all buffered points and clears the local buffer.
func (p *Poller) Points() (res []*Point) {
	p.lock.Lock()
	res		= p.points
	p.points	= p.points[:0]
	p.lock.Unlock()

	return
}

// Reads, decodes and transforms target values every conf.PollInterval.
// The modbus link (either TCP or RTU) is reconnected automaticaly whenever an
// unrecoverable i/o error is encountered (i.e. if the error is neither a timeout nor
// a modbus error).
// Values read are stored as Point objects in an internal slice and can be collected
// using the Points() method above.
func (p *Poller) poll() {
	var ticker		*time.Ticker
	var err			error
	var mcOpen		bool
	var failedAttempts	uint
	var value		interface{}
	var u16			uint16
	var u32			uint32
	var f64			float64

	ticker	= time.NewTicker(p.conf.PollInterval)

	for {
		<-ticker.C

		if !mcOpen {
			err = p.mc.Open()
			if err != nil {
				fmt.Printf("failed to open modbus link %s: %v\n",
					   p.conf.Url, err)

				// sleep for a capped exponential time to avoid hammering the
				// network/serial line with retries
				if failedAttempts < 5 {
					failedAttempts++
				}
				time.Sleep(time.Duration(failedAttempts * 5) * time.Second)

				continue
			}
			failedAttempts	= 0
			mcOpen		= true
			fmt.Printf("modbus link %s open\n", p.conf.Url)
		}

		for _, target := range p.conf.Targets {

			// set the modbus unit ID
			p.mc.SetUnitId(target.UnitId)

			switch target.ValueType {
			case UINT16, INT16:
				u16, err   = p.mc.ReadRegister(target.RegAddr, target.MbType)

				// decode as signed if required
				if target.ValueType == INT16 {
					value	= int16(u16)
				} else {
					value	= u16
				}

			case UINT32, INT32:
				u32, err   = p.mc.ReadUint32(target.RegAddr, target.MbType)

				// decode as signed if required
				if target.ValueType == INT32 {
					value	= int32(u32)
				} else {
					value	= u32
				}

			case FLOAT32:
				value, err = p.mc.ReadFloat32(target.RegAddr, target.MbType)

			default:
				fmt.Printf("unsupported register type '%v'\n", target.ValueType)
			}

			if err != nil {
				fmt.Printf("failed to read target '%s': %v\n",
				           target.Label, err)

				// if the error is most likely not recoverable,
				// close the modbus link to force a reconnect
				if !isRecoverableError(err) {
					p.mc.Close()
					mcOpen	= false
					break
				}

				continue
			}

			// apply transforms, if any
			// note: any use of Offset, ScaleFactor or DecimalPlaces
			// converts the value to float64
			if target.ScaleFactor != 0 ||
			   target.Offset != 0 ||
			   target.DecimalPlaces != 0 {

				// type conversion
				switch value.(type) {
				case uint16:	f64	= float64(value.(uint16))
				case int16:	f64	= float64(value.(int16))
				case int32:	f64	= float64(value.(int32))
				case uint32:	f64	= float64(value.(uint32))
				case float32:	f64	= float64(value.(float32))
				}

				// apply the scale factor
				if target.ScaleFactor != 0 {
					f64	*= target.ScaleFactor
				}

				// apply the offset
				if target.Offset != 0 {
					f64	+= target.Offset
				}

				// round to target.DecimalPlaces decimal places
				if target.DecimalPlaces != 0 {
					f64	= round(f64, target.DecimalPlaces)
				}

				value	= f64
			}

			p.lock.Lock()
			// turn the value into a Point object and add it to the poller's
			// internal buffer
			p.points = append(p.points,
				&Point{
					Timestamp:	time.Now().UTC(),
					Label:		target.Label,
					Value:		value,
				})
			p.lock.Unlock()
		}
	}

	return
}

// Rounds a value to the specified number of decimal places.
func round(val float64, places uint) (res float64) {
	var shift	float64 = math.Pow10(int(places))
	if val >= 0 {
		res = math.Floor((val * shift) + 0.5) / shift
	} else {
		res = math.Floor((val * shift) - 0.5) / shift
	}

	return
}

// Returns true if the error is recoverable in the sense that
// it does not require the poller to re-establish the modbus link.
func isRecoverableError(err error) (yes bool) {
	if err == modbus.ErrIllegalFunction ||
	   err == modbus.ErrIllegalDataAddress ||
	   err == modbus.ErrIllegalDataValue ||
	   err == modbus.ErrServerDeviceFailure ||
	   err == modbus.ErrMemoryParityError ||
	   err == modbus.ErrServerDeviceBusy ||
	   err == modbus.ErrGWPathUnavailable ||
	   err == modbus.ErrGWTargetFailedToRespond ||
	   err == modbus.ErrRequestTimedOut ||
	   os.IsTimeout(err) {
		   yes = true
	   }

	return
}
