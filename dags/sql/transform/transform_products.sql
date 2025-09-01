ALTER TABLE staging.products
ADD COLUMN brand_name TEXT;

UPDATE staging.products
SET brand_name = SUBSTRING(
        product_name 
        FROM POSITION('(' IN product_name) + 1
        FOR POSITION(')' IN product_name) - POSITION('(' IN product_name) - 1
    ),
    product_name = REGEXP_REPLACE(
        TRIM(SUBSTRING(
            product_name
            FROM 1 
            FOR POSITION('(' IN product_name) - 1
        )),
        '\s+',
        ' ',
        'g'
    );


DELETE FROM staging.products
WHERE product_name IS NULL OR brand_name IS NULL ;

