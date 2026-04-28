CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS paymentorder_payment_trade_no_unique
    ON payment_orders(payment_trade_no)
    WHERE payment_trade_no <> '';
