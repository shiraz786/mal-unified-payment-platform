-- 1) Daily payment volume by product
SELECT DATE(event_timestamp) AS payment_date,
       payment_type,
       COUNT(*) AS txn_count,
       SUM(amount) AS total_amount
FROM unified_payments
GROUP BY 1,2
ORDER BY 1,2;

-- 2) Failed payments that need operations follow-up
SELECT event_id, payment_type, customer_id, amount, currency, status, raw_source
FROM unified_payments
WHERE status IN ('FAILED', 'REVERSED')
ORDER BY event_timestamp DESC;

-- 3) International card spend monitoring
SELECT customer_id,
       COUNT(*) AS international_txn_count,
       SUM(amount) AS international_spend
FROM unified_payments
WHERE payment_type = 'CARD' AND is_international = TRUE
GROUP BY customer_id
ORDER BY international_spend DESC;

-- 4) Biller performance view
SELECT biller_name,
       COUNT(*) AS bill_count,
       SUM(amount) AS amount_collected
FROM unified_payments
WHERE payment_type = 'BILL_PAYMENT' AND status = 'SUCCESS'
GROUP BY biller_name
ORDER BY amount_collected DESC;

-- 5) Transfer rail analytics
SELECT payment_method,
       COUNT(*) AS txn_count,
       AVG(amount) AS avg_amount
FROM unified_payments
WHERE payment_type = 'TRANSFER'
GROUP BY payment_method
ORDER BY txn_count DESC;
