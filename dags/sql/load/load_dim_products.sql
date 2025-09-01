INSERT INTO core.dim_products (product_id, product_name, category, subcategory, brand_name)
SELECT product_id, product_name, category, subcategory, brand_name
FROM staging.products
ON CONFLICT (product_id) DO UPDATE SET
    product_name = EXCLUDED.product_name,
    category = EXCLUDED.category,
    subcategory = EXCLUDED.subcategory,
    brand_name = EXCLUDED.brand_name;