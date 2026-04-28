package service

import (
	"context"
	"testing"
	"time"
)

func TestPaymentTradeNoUsedByAnotherOrder(t *testing.T) {
	ctx := context.Background()
	client := newPaymentConfigServiceTestClient(t)

	user, err := client.User.Create().
		SetEmail("trade-no-user@example.com").
		SetPasswordHash("hash").
		SetUsername("trade-no-user").
		Save(ctx)
	if err != nil {
		t.Fatalf("create user: %v", err)
	}

	orderA, err := client.PaymentOrder.Create().
		SetUserID(user.ID).
		SetUserEmail(user.Email).
		SetUserName(user.Username).
		SetAmount(10).
		SetPayAmount(10).
		SetFeeRate(0).
		SetRechargeCode("TRADE-NO-A").
		SetOutTradeNo("sub2_trade_no_a").
		SetPaymentType("alipay").
		SetPaymentTradeNo("shared-trade-no").
		SetOrderType("balance").
		SetStatus(OrderStatusCompleted).
		SetExpiresAt(time.Now().Add(time.Hour)).
		SetPaidAt(time.Now()).
		SetClientIP("127.0.0.1").
		SetSrcHost("api.example.com").
		Save(ctx)
	if err != nil {
		t.Fatalf("create orderA: %v", err)
	}

	orderB, err := client.PaymentOrder.Create().
		SetUserID(user.ID).
		SetUserEmail(user.Email).
		SetUserName(user.Username).
		SetAmount(12).
		SetPayAmount(12).
		SetFeeRate(0).
		SetRechargeCode("TRADE-NO-B").
		SetOutTradeNo("sub2_trade_no_b").
		SetPaymentType("alipay").
		SetPaymentTradeNo("").
		SetOrderType("balance").
		SetStatus(OrderStatusPending).
		SetExpiresAt(time.Now().Add(time.Hour)).
		SetClientIP("127.0.0.1").
		SetSrcHost("api.example.com").
		Save(ctx)
	if err != nil {
		t.Fatalf("create orderB: %v", err)
	}

	svc := &PaymentService{entClient: client}

	reused, err := svc.paymentTradeNoUsedByAnotherOrder(ctx, orderB.ID, "shared-trade-no")
	if err != nil {
		t.Fatalf("paymentTradeNoUsedByAnotherOrder returned error: %v", err)
	}
	if !reused {
		t.Fatalf("expected shared trade_no to be marked as reused")
	}

	reused, err = svc.paymentTradeNoUsedByAnotherOrder(ctx, orderA.ID, "shared-trade-no")
	if err != nil {
		t.Fatalf("paymentTradeNoUsedByAnotherOrder self-check returned error: %v", err)
	}
	if reused {
		t.Fatalf("expected same order to be ignored when checking trade_no reuse")
	}
}
