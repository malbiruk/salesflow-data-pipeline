CREATE TABLE IF NOT EXISTS region (region VARCHAR(255) PRIMARY KEY);

CREATE TABLE IF NOT EXISTS country (
    country VARCHAR(255) PRIMARY KEY,
    region VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS product (
    item_type VARCHAR(255) PRIMARY KEY,
    unit_cost FLOAT,
    unit_price FLOAT
);

CREATE TABLE IF NOT EXISTS order_priority (order_priority VARCHAR(255) PRIMARY KEY);

CREATE TABLE IF NOT EXISTS orders (
    id VARCHAR(255) PRIMARY KEY,
    source_order_id INTEGER,
    country VARCHAR(255) NOT NULL,
    is_online BOOLEAN,
    order_priority VARCHAR(255) NOT NULL,
    product_id VARCHAR(255) NOT NULL,
    units_sold INTEGER,
    order_date DATE,
    ship_date DATE
);

ALTER TABLE country ADD FOREIGN KEY (region) REFERENCES region (region);

ALTER TABLE orders ADD FOREIGN KEY (country) REFERENCES country (country);

ALTER TABLE orders ADD FOREIGN KEY (order_priority) REFERENCES order_priority (order_priority);

ALTER TABLE orders ADD FOREIGN KEY (product_id) REFERENCES product (item_type);
