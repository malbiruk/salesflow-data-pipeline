CREATE TABLE region (region VARCHAR(255) PRIMARY KEY);

CREATE TABLE country (
    country VARCHAR(255) PRIMARY KEY,
    region VARCHAR(255) NOT NULL
);

CREATE TABLE product (
    item_type VARCHAR(255) PRIMARY KEY,
    unit_cost FLOAT,
    unit_price FLOAT
);

CREATE TABLE order_priority (order_priority VARCHAR(255) PRIMARY KEY);

CREATE TABLE "order" (
    id INTEGER,
    country VARCHAR(255) NOT NULL,
    is_online BOOLEAN,
    order_priority VARCHAR(255) NOT NULL,
    product_id VARCHAR(255),
    units_sold INTEGER,
    order_date DATE,
    ship_date DATE
);

ALTER TABLE country ADD FOREIGN KEY (region) REFERENCES region (region);

ALTER TABLE "order" ADD FOREIGN KEY (country) REFERENCES country (country);

ALTER TABLE "order" ADD FOREIGN KEY (order_priority) REFERENCES order_priority (order_priority);

ALTER TABLE "order" ADD FOREIGN KEY (product_id) REFERENCES product (item_type);
