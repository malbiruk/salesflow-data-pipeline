CREATE TABLE IF NOT EXISTS raw_sales_data (
    region VARCHAR(255),
    country VARCHAR(255),
    item_type VARCHAR(255),
    sales_channel VARCHAR(255),
    order_priority VARCHAR(255),
    order_date DATE,
    order_id INTEGER,
    ship_date DATE,
    units_sold INTEGER,
    unit_price FLOAT,
    unit_cost FLOAT
);
