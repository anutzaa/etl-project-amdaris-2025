# Data warehouse schema
USE warehouse;


DROP TABLE IF EXISTS fact_btc;
DROP TABLE IF EXISTS fact_gold;
DROP TABLE IF EXISTS fact_exchange_rates;
DROP TABLE IF EXISTS dim_date;


CREATE TABLE dim_date(
    date DATE PRIMARY KEY,
    day INT NOT NULL,
    month INT NOT NULL,
    month_name VARCHAR(15) NOT NULL,
    quarter INT NOT NULL,
    year INT NOT NULL,
    day_of_week INT NOT NULL,
    week_of_year INT NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    created_at TIMESTAMP(4),
    updated_at TIMESTAMP(4)
);


CREATE TABLE fact_btc(
    Id INT AUTO_INCREMENT PRIMARY KEY,
    date DATE NOT NULL,
    currency_id INT,
    open DECIMAL(16,2) NOT NULL,
    high DECIMAL(16,2) NOT NULL,
    low DECIMAL(16,2) NOT NULL,
    close DECIMAL(16,2) NOT NULL,
    volume DECIMAL (20,8) NOT NULL,
    created_at TIMESTAMP(4) NOT NULL,
    updated_at TIMESTAMP(4) NOT NULL,
    FOREIGN KEY (currency_id) REFERENCES dim_currency(Id) ON DELETE SET NULL,
    FOREIGN KEY (date) REFERENCES dim_date(date),
    UNIQUE INDEX idx_currency_date (currency_id, date)
);


CREATE TABLE fact_gold(
    Id INT AUTO_INCREMENT PRIMARY KEY,
    currency_id INT,
    date DATE NOT NULL,
    open DECIMAL(16,2) NOT NULL,
    high DECIMAL(16,2) NOT NULL,
    low DECIMAL(16,2) NOT NULL,
    price DECIMAL(16,2) NOT NULL,
    price_24k DECIMAL(16,8),
    price_18k DECIMAL(16,8),
    price_14k DECIMAL(16,8),
    created_at TIMESTAMP(4) NOT NULL,
    updated_at TIMESTAMP(4) NOT NULL,
    FOREIGN KEY (currency_id) REFERENCES dim_currency(Id) ON DELETE SET NULL,
    FOREIGN KEY (date) REFERENCES dim_date(date),
    UNIQUE INDEX idx_currency_date (currency_id, date)
);


CREATE TABLE fact_exchange_rates(
    Id INT AUTO_INCREMENT PRIMARY KEY,
    date DATE NOT NULL,
    base_currency_id INT NOT NULL,
    target_currency_id INT NOT NULL,
    rate DECIMAL(6,5) NOT NULL,
    created_at TIMESTAMP(4) NOT NULL,
    updated_at TIMESTAMP(4) NOT NULL,
    FOREIGN KEY (base_currency_id) REFERENCES dim_currency(Id),
    FOREIGN KEY (target_currency_id) REFERENCES dim_currency(Id),
    FOREIGN KEY (date) REFERENCES dim_date(date),
    UNIQUE INDEX idx_currency_date (base_currency_id, target_currency_id, date)
);