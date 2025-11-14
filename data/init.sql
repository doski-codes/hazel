\c hazel_db;

DROP TABLE IF EXISTS dim_country;
DROP TABLE IF EXISTS dim_model;
DROP TABLE IF EXISTS dim_ordertype;
DROP TABLE IF EXISTS fact_sales_ordertype;
DROP TABLE IF EXISTS fact_sales;

CREATE TABLE dim_country (
    country VARCHAR(100) UNIQUE NOT NULL,
    country_code VARCHAR(2) PRIMARY KEY,
    region VARCHAR(100) NOT NULL
);

COPY dim_country
FROM '/docker-entrypoint-initdb.d/DIM_COUNTRY.csv'
DELIMITER ','
CSV HEADER;


CREATE TABLE dim_model (
    model_id INT PRIMARY KEY,
    model_name VARCHAR(20) NOT NULL,
    brand VARCHAR(20) NOT NULL,
    segment VARCHAR(20),
    powertrain VARCHAR(20)
);

COPY dim_model
FROM '/docker-entrypoint-initdb.d/DIM_MODEL.csv'
DELIMITER ','
CSV HEADER;


CREATE TABLE dim_ordertype (
    ordertype_id INT PRIMARY KEY,
    ordertype_name VARCHAR(10) UNIQUE NOT NULL,
    "description" VARCHAR(60) NOT NULL
);

COPY dim_ordertype
FROM '/docker-entrypoint-initdb.d/DIM_ORDERTYPE.csv'
DELIMITER ','
CSV HEADER;


CREATE TABLE fact_sales_ordertype (
    model_id INT REFERENCES dim_model (model_id) NOT NULL,
    country_code VARCHAR(2) REFERENCES dim_country (country_code) NOT NULL,
    "year" VARCHAR(4) NOT NULL,
    "month" INT NOT NULL,
    contracts INT NOT NULL,
    ordertype_id INT REFERENCES dim_ordertype (ordertype_id) NOT NULL
);

COPY fact_sales_ordertype
FROM '/docker-entrypoint-initdb.d/FACT_SALES_ORDERTYPE.csv'
DELIMITER ','
CSV HEADER;


CREATE TABLE fact_sales (
    model_id INT REFERENCES dim_model (model_id) NOT NULL,
    country_code VARCHAR(2) REFERENCES dim_country (country_code) NOT NULL,
    "year" VARCHAR(4) NOT NULL,
    "month" INT NOT NULL,
    contracts INT NOT NULL
);

COPY fact_sales
FROM '/docker-entrypoint-initdb.d/FACT_SALES.csv'
DELIMITER ','
CSV HEADER;