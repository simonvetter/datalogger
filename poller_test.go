package main

import (
	"math"
	"testing"
)

func TestPollerRound(t *testing.T) {
	var res	float64

	res = round(0.1234567, 4)
	if res != 0.1235 {
		t.Errorf("expected %f, got: %f", 0.1235, res)
	}

	res = round(0.8234567, 5)
	if res != 0.82346 {
		t.Errorf("expected %f, got: %f", 0.82346, res)
	}

	res = round(-0.8234567, 3)
	if res != -0.824 {
		t.Errorf("expected %f, got: %f", -0.824, res)
	}

	res = round(-100.8234567, 0)
	if res != -102 {
		t.Errorf("expected %f, got: %f", -102.0, res)
	}

	res = round(math.NaN(), 4)
	if !math.IsNaN(res) {
		t.Errorf("expected %f, got: %f", math.NaN(), res)
	}

	res = round(math.Inf(0), 2)
	if !math.IsInf(res, 0) {
		t.Errorf("expected %f, got: %f", math.Inf(0), res)
	}

	return
}
