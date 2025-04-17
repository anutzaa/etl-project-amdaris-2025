# Data warehouse schema
CREATE SCHEMA warehouse;
USE warehouse;

DROP TABLE IF EXISTS dim_currency;

CREATE TABLE dim_currency (
    Id INT AUTO_INCREMENT PRIMARY KEY,
    code VARCHAR(3) UNIQUE NOT NULL,
    name VARCHAR(50) UNIQUE NOT NULL
);

INSERT INTO dim_currency(code, name)
VALUES('EUR', 'Euro'),
      ('USD', 'United States Dollar'),
      ('GBP', 'British Pound Sterling'),
      ('AED','United Arab Emirates Dirham'),
      ('BGN','Bulgarian Lev'),
      ('CAD','Canadian Dollar'),
      ('CHF','Swiss Franc'),
      ('RON','Romanian Leu'),
      ('TRY','Turkish Lira'),
      ('UAH','Ukrainian Hryvnia');


# Extract schema
CREATE SCHEMA extract;
USE extract;


DROP TABLE IF EXISTS api;
DROP TABLE IF EXISTS api_import_log;
DROP TABLE IF EXISTS import_log;


CREATE TABLE api(
    Id VARCHAR(3) PRIMARY KEY,
    name VARCHAR(10) UNIQUE NOT NULL
);


INSERT INTO api
VALUES ('BTC','BitcoinAPI'),
       ('XAU', 'GoldAPI');


CREATE TABLE import_log(
    Id INT AUTO_INCREMENT PRIMARY KEY,
    batch_date DATE NOT NULL,
    currency_id INT,
    import_directory_name VARCHAR(50) NOT NULL,
    import_file_name VARCHAR(50) NOT NULL UNIQUE,
    file_created_date TIMESTAMP(4) NOT NULL,
    file_last_modified_date TIMESTAMP(4),
    row_count INT,
    FOREIGN KEY (currency_id) REFERENCES warehouse.dim_currency(Id) ON DELETE SET NULL
);


CREATE TABLE api_import_log(
    Id INT AUTO_INCREMENT PRIMARY KEY,
    currency_id INT,
    api_id VARCHAR(3),
    start_time TIMESTAMP(4) NOT NULL,
    end_time TIMESTAMP(4),
    code_response SMALLINT,
    error_messages VARCHAR(255),
    FOREIGN KEY (api_id) REFERENCES api(Id) ON DELETE SET NULL,
    FOREIGN KEY (currency_id) REFERENCES warehouse.dim_currency(Id) ON DELETE SET NULL
);