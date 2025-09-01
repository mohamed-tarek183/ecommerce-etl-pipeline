DELETE FROM staging.sales
WHERE payment IS NULL;

ALTER TABLE staging.sales
    ALTER COLUMN loyalty_card TYPE boolean
        USING CASE
            WHEN loyalty_card = 'T' THEN TRUE
            WHEN loyalty_card = 'F' THEN FALSE
            ELSE NULL
        END,
    DROP COLUMN credit_card;





