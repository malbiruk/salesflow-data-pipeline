CREATE TABLE IF NOT EXISTS raw_sales_data (
    id INTEGER AUTOINCREMENT PRIMARY KEY,
    region VARCHAR(255),
    country VARCHAR(255),
    item_type VARCHAR(255),
    sales_channel VARCHAR(255),
    order_priority VARCHAR(255),
    order_date VARCHAR(255),
    order_id VARCHAR(255),
    ship_date VARCHAR(255),
    units_sold INTEGER,
    unit_price FLOAT,
    unit_cost FLOAT
);
