package moneyx

import "testing"

func TestMoneyPrecisionForReferralAndRefundMath(t *testing.T) {
	if got := Currency(0.1 + 0.2).StringFixed(2); got != "0.30" {
		t.Fatalf("Currency(0.1 + 0.2) = %s, want 0.30", got)
	}
	if got := MultiplyRate(0.30, 10, ScaleCommission).StringFixed(8); got != "0.03000000" {
		t.Fatalf("MultiplyRate(0.30, 10) = %s, want 0.03000000", got)
	}
	if got := Proportion(0.03, 0.1, 0.3, ScaleCommission).StringFixed(8); got != "0.01000000" {
		t.Fatalf("Proportion(0.03, 0.1, 0.3) = %s, want 0.01000000", got)
	}
	if got := MultiplyRate(99.99, 12.5, ScaleCommission).StringFixed(8); got != "12.49875000" {
		t.Fatalf("MultiplyRate(99.99, 12.5) = %s, want 12.49875000", got)
	}
	if got := MultiplyRate(0.30, 33.33, ScaleCommission).StringFixed(8); got != "0.09999000" {
		t.Fatalf("MultiplyRate(0.30, 33.33) = %s, want 0.09999000", got)
	}
	if got := Proportion(9.999, 10.00, 30.00, ScaleCommission).StringFixed(8); got != "3.33300000" {
		t.Fatalf("Proportion partial refund = %s, want 3.33300000", got)
	}
}
