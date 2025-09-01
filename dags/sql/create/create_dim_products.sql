CREATE TABLE IF NOT EXISTS core.dim_products (
                    product_PK INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    product_id varchar(5) UNIQUE,
                    product_name varchar(100),
                    category varchar(50),
                    subcategory varchar(50),
                    brand_name varchar(50)

                );