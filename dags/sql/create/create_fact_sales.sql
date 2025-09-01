CREATE TABLE IF NOT EXISTS core.fact_sales (
     transaction_PK integer PRIMARY KEY,
     product_FK INT NOT NULL REFERENCES core.dim_products(product_PK) ,
     payment_FK INT NOT NULL REFERENCES core.dim_payment(junk_PK),
     date_FK INT NOT NULL REFERENCES core.dim_date(date_PK) ,
     customer_id INT,
     cost numeric,
     quantity INT,
     price numeric
)