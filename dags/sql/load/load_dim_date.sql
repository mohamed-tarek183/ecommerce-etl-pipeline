-- Step 1: Set the start and end dates
DO $$
DECLARE
    start_date DATE := '1990-01-01';
    end_date DATE := '2050-12-31';
BEGIN
    -- Step 2: Use GENERATE_SERIES to create and insert all the dates
    INSERT INTO core.dim_date (
        date_PK,
        full_date,
        day_of_week,
        day_name_of_week,
        day_of_month,
        day_of_year,
        week_of_year,
        month_of_year,
        month_name,
        quarter_of_year,
        quarter_name,
        year,
        is_weekend,
        is_holiday
    )
    SELECT
        -- DateKey in YYYYMMDD format
        CAST(TO_CHAR(day_series, 'YYYYMMDD') AS INT),
        day_series::DATE,
        EXTRACT(DOW FROM day_series), -- DOW = day of week (0=Sunday, 6=Saturday)
        TRIM(TO_CHAR(day_series, 'Day')),
        EXTRACT(DAY FROM day_series),
        EXTRACT(DOY FROM day_series),
        EXTRACT(WEEK FROM day_series),
        EXTRACT(MONTH FROM day_series),
        TRIM(TO_CHAR(day_series, 'Month')),
        EXTRACT(QUARTER FROM day_series),
        'Q' || EXTRACT(QUARTER FROM day_series)::TEXT,
        EXTRACT(YEAR FROM day_series),
        -- IsWeekend: DOW=0 for Sunday and 6 for Saturday
        CASE WHEN EXTRACT(DOW FROM day_series) IN (0, 6) THEN TRUE ELSE FALSE END,
        FALSE -- Placeholder for holiday logic
    FROM generate_series(start_date, end_date, '1 day'::interval) AS day_series;
END $$;