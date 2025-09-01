INSERT INTO core.dim_payment (payment, loyalty_card)
SELECT DISTINCT payment, loyalty_card
FROM staging.sales
ON CONFLICT (payment, loyalty_card) DO NOTHING;