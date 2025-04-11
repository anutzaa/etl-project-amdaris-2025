# Transform schema
CREATE SCHEMA transform;
USE transform;


DROP TABLE IF EXISTS transform_log;
DROP TABLE IF EXISTS btc_data_import;
DROP TABLE IF EXISTS gold_data_import;


CREATE TABLE transform_log(
    Id INT AUTO_INCREMENT PRIMARY KEY,
    batch_date DATE NOT NULL,
    currency_id INT,
    processed_directory_name VARCHAR(50) NOT NULL,
    processed_file_name VARCHAR(50) NOT NULL,
    row_count INT,
    status VARCHAR(15),
    FOREIGN KEY (currency_id) REFERENCES warehouse.dim_currency(Id) ON DELETE SET NULL
);


CREATE TABLE btc_data_import(
    Id INT AUTO_INCREMENT PRIMARY KEY,
    currency_id INT,
    date DATE NOT NULL,
    open DECIMAL(16,2) NOT NULL,
    high DECIMAL(16,2) NOT NULL,
    low DECIMAL(16,2) NOT NULL,
    close DECIMAL(16,2) NOT NULL,
    volume DECIMAL (20,8) NOT NULL,
    FOREIGN KEY (currency_id) REFERENCES warehouse.dim_currency(Id) ON DELETE SET NULL,
    UNIQUE INDEX idx_currency_DATE (currency_id, date)
);


CREATE TABLE gold_data_import(
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
    rate_usd DECIMAL(6,5),
    rate_eur DECIMAL(6,5),
    rate_gbp DECIMAL(6,5),
    FOREIGN KEY (currency_id) REFERENCES warehouse.dim_currency(Id) ON DELETE SET NULL,
    UNIQUE INDEX idx_currency_DATE (currency_id, date)
);