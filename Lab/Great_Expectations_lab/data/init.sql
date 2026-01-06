-- Initialize sample data for Great Expectations lab

-- Create schema
CREATE SCHEMA IF NOT EXISTS raw;

-- Create sample tables
CREATE TABLE IF NOT EXISTS raw.customers (
    customer_id INTEGER PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    age INTEGER,
    registration_date DATE,
    country VARCHAR(50),
    total_orders INTEGER DEFAULT 0,
    lifetime_value DECIMAL(10, 2) DEFAULT 0.00
);

CREATE TABLE IF NOT EXISTS raw.orders (
    order_id INTEGER PRIMARY KEY,
    customer_id INTEGER,
    order_date DATE,
    total_amount DECIMAL(10, 2),
    status VARCHAR(20),
    product_count INTEGER,
    FOREIGN KEY (customer_id) REFERENCES raw.customers(customer_id)
);

-- Insert sample data
INSERT INTO raw.customers (customer_id, first_name, last_name, email, age, registration_date, country, total_orders, lifetime_value) VALUES
(1, 'John', 'Doe', 'john.doe@email.com', 30, '2023-01-15', 'USA', 5, 1500.00),
(2, 'Jane', 'Smith', 'jane.smith@email.com', 25, '2023-02-20', 'Canada', 3, 800.00),
(3, 'Bob', 'Johnson', 'bob.johnson@email.com', 35, '2023-03-10', 'USA', 8, 2500.00),
(4, 'Alice', 'Williams', 'alice.williams@email.com', 28, '2023-04-05', 'UK', 2, 600.00),
(5, 'Charlie', 'Brown', 'charlie.brown@email.com', 32, '2023-05-12', 'USA', 6, 1800.00)
ON CONFLICT (customer_id) DO NOTHING;

INSERT INTO raw.orders (order_id, customer_id, order_date, total_amount, status, product_count) VALUES
(1, 1, '2024-01-10', 300.00, 'completed', 2),
(2, 2, '2024-01-15', 150.00, 'completed', 1),
(3, 1, '2024-01-20', 200.00, 'completed', 3),
(4, 3, '2024-02-01', 500.00, 'pending', 5),
(5, 4, '2024-02-05', 100.00, 'completed', 1),
(6, 3, '2024-02-10', 400.00, 'completed', 4),
(7, 5, '2024-02-15', 250.00, 'completed', 2),
(8, 1, '2024-02-20', 350.00, 'completed', 3)
ON CONFLICT (order_id) DO NOTHING;

