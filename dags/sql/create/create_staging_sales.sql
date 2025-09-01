 CREATE TABLE IF NOT EXISTS staging.sales (
                    transaction_id integer PRIMARY KEY,
                    transactional_date timestamp,
                    product_id varchar,
                    customer_id integer,
                    payment varchar,
                    credit_card bigint,
                    loyalty_card varchar,
                    cost numeric,
                    quantity integer,
                    price numeric
 );