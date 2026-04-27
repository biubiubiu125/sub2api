package moneyx

import (
	"math"

	"github.com/shopspring/decimal"
)

const (
	ScaleCurrency   int32 = 2
	ScaleRate       int32 = 4
	ScaleCommission int32 = 8
)

// Decimal is the boundary adapter for legacy float64 Ent/API values.
// Core referral money calculations should convert through this package before
// arithmetic, then round explicitly before returning to those legacy boundaries.
func Decimal(v float64, scale int32) decimal.Decimal {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return decimal.Zero
	}
	return decimal.NewFromFloat(v).Round(scale)
}

func Currency(v float64) decimal.Decimal {
	return Decimal(v, ScaleCurrency)
}

func Rate(v float64) decimal.Decimal {
	return Decimal(v, ScaleRate)
}

func Commission(v float64) decimal.Decimal {
	return Decimal(v, ScaleCommission)
}

func Round(v float64, scale int32) float64 {
	return Decimal(v, scale).InexactFloat64()
}

func Min(a, b decimal.Decimal) decimal.Decimal {
	if a.Cmp(b) <= 0 {
		return a
	}
	return b
}

func NonNegative(v decimal.Decimal) decimal.Decimal {
	if v.IsNegative() {
		return decimal.Zero
	}
	return v
}

func Equal(a, b float64, scale int32) bool {
	return Decimal(a, scale).Equal(Decimal(b, scale))
}

func MultiplyRate(baseAmount float64, rate float64, scale int32) decimal.Decimal {
	return Decimal(baseAmount, scale).
		Mul(Rate(rate)).
		Div(decimal.NewFromInt(100)).
		Round(scale)
}

func Proportion(total float64, numerator float64, denominator float64, scale int32) decimal.Decimal {
	denominatorDec := Decimal(denominator, scale)
	if denominatorDec.IsZero() {
		return decimal.Zero
	}
	return Decimal(total, scale).
		Mul(Decimal(numerator, scale)).
		Div(denominatorDec).
		Round(scale)
}
