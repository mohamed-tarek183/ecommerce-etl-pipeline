CREATE TABLE IF NOT EXISTS core.dim_date (
    date_PK INT PRIMARY KEY,
    full_date DATE,
    day_of_week INT,
    day_name_of_week VARCHAR(10),
    day_of_month INT,
    day_of_year INT,
    week_of_year INT,
    month_of_year INT,
    month_name VARCHAR(10),
    quarter_of_year INT,
    quarter_name VARCHAR(10),
    year INT,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN
);