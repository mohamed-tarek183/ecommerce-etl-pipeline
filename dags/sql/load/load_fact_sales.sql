INSERT INTO core.fact_sales (transaction_PK, product_FK, payment_FK, date_FK, customer_id, cost, quantity, price)
SELECT
    s.transaction_id,
    pd.product_PK,
    pt.junk_PK,
    d.date_PK,
    s.customer_id,
    s.cost,
    s.quantity,
    s.price
FROM
    staging.sales s
INNER JOIN
    core.dim_products pd ON s.product_id = pd.product_id
INNER JOIN
    core.dim_payment pt ON s.payment = pt.payment AND s.loyalty_card = pt.loyalty_card
INNER JOIN
    core.dim_date d ON CAST(s.transactional_date AS DATE) = d.full_date;
