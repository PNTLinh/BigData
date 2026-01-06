-- Initialize sample data for dbt lab

-- Create schema for raw data
CREATE SCHEMA IF NOT EXISTS raw;

-- Create sample tables
CREATE TABLE IF NOT EXISTS raw.customers (
    customer_id INTEGER PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    registration_date DATE,
    country VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS raw.orders (
    order_id INTEGER PRIMARY KEY,
    customer_id INTEGER,
    order_date DATE,
    total_amount DECIMAL(10, 2),
    status VARCHAR(20),
    FOREIGN KEY (customer_id) REFERENCES raw.customers(customer_id)
);

CREATE TABLE IF NOT EXISTS raw.products (
    product_id INTEGER PRIMARY KEY,
    product_name VARCHAR(100),
    category VARCHAR(50),
    price DECIMAL(10, 2),
    stock_quantity INTEGER
);

CREATE TABLE IF NOT EXISTS raw.order_items (
    order_item_id INTEGER PRIMARY KEY,
    order_id INTEGER,
    product_id INTEGER,
    quantity INTEGER,
    unit_price DECIMAL(10, 2),
    FOREIGN KEY (order_id) REFERENCES raw.orders(order_id),
    FOREIGN KEY (product_id) REFERENCES raw.products(product_id)
);

-- Insert sample data
INSERT INTO raw.customers (customer_id, first_name, last_name, email, registration_date, country) VALUES
(1, 'John', 'Doe', 'john.doe@email.com', '2023-01-15', 'USA'),
(2, 'Jane', 'Smith', 'jane.smith@email.com', '2023-02-20', 'Canada'),
(3, 'Bob', 'Johnson', 'bob.johnson@email.com', '2023-03-10', 'USA'),
(4, 'Alice', 'Williams', 'alice.williams@email.com', '2023-04-05', 'UK'),
(5, 'Charlie', 'Brown', 'charlie.brown@email.com', '2023-05-12', 'USA')
ON CONFLICT (customer_id) DO NOTHING;

INSERT INTO raw.products (product_id, product_name, category, price, stock_quantity) VALUES
(1, 'Laptop', 'Electronics', 999.99, 50),
(2, 'Mouse', 'Electronics', 29.99, 200),
(3, 'Keyboard', 'Electronics', 79.99, 150),
(4, 'Monitor', 'Electronics', 299.99, 75),
(5, 'Desk Chair', 'Furniture', 199.99, 30)
ON CONFLICT (product_id) DO NOTHING;

INSERT INTO raw.orders (order_id, customer_id, order_date, total_amount, status) VALUES
(1, 1, '2024-01-10', 999.99, 'completed'),
(2, 2, '2024-01-15', 129.98, 'completed'),
(3, 1, '2024-01-20', 299.99, 'completed'),
(4, 3, '2024-02-01', 79.99, 'pending'),
(5, 4, '2024-02-05', 199.99, 'completed')
ON CONFLICT (order_id) DO NOTHING;

INSERT INTO raw.order_items (order_item_id, order_id, product_id, quantity, unit_price) VALUES
(1, 1, 1, 1, 999.99),
(2, 2, 2, 2, 29.99),
(3, 2, 3, 1, 79.99),
(4, 3, 4, 1, 299.99),
(5, 4, 3, 1, 79.99),
(6, 5, 5, 1, 199.99)
ON CONFLICT (order_item_id) DO NOTHING;

