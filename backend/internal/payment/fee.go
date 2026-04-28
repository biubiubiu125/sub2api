package payment

import (
	"github.com/Wei-Shaw/sub2api/internal/pkg/moneyx"
	"github.com/shopspring/decimal"
)

// CalculatePayAmount computes the total pay amount given a recharge amount and
// fee rate (percentage). Fee = amount * feeRate / 100, rounded UP (away from zero)
// to 2 decimal places. The returned string is formatted to exactly 2 decimal places.
// If feeRate <= 0, the amount is returned as-is (formatted to 2 decimal places).
func CalculatePayAmount(rechargeAmount float64, feeRate float64) string {
	amount := moneyx.Currency(rechargeAmount)
	if feeRate <= 0 {
		return amount.StringFixed(2)
	}
	rate := moneyx.Rate(feeRate)
	fee := amount.Mul(rate).Div(decimal.NewFromInt(100)).RoundUp(2)
	return amount.Add(fee).StringFixed(2)
}
