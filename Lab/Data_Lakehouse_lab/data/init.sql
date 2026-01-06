-- Initialize databases for Data Lakehouse Lab

-- Create schemas
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;

-- Raw data tables
CREATE TABLE IF NOT EXISTS raw.events (
    event_id SERIAL PRIMARY KEY,
    user_id INTEGER,
    event_type VARCHAR(50),
    event_timestamp TIMESTAMP,
    properties JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw.users (
    user_id INTEGER PRIMARY KEY,
    username VARCHAR(100),
    email VARCHAR(100),
    registration_date DATE,
    country VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert sample data
INSERT INTO raw.users (user_id, username, email, registration_date, country) VALUES
(1, 'user1', 'user1@example.com', '2024-01-01', 'USA'),
(2, 'user2', 'user2@example.com', '2024-01-02', 'Canada'),
(3, 'user3', 'user3@example.com', '2024-01-03', 'UK')
ON CONFLICT (user_id) DO NOTHING;

INSERT INTO raw.events (user_id, event_type, event_timestamp, properties) VALUES
(1, 'page_view', '2024-01-10 10:00:00', '{"page": "/home", "duration": 30}'),
(1, 'purchase', '2024-01-10 10:05:00', '{"product_id": 101, "amount": 99.99}'),
(2, 'page_view', '2024-01-10 11:00:00', '{"page": "/products", "duration": 60}'),
(3, 'signup', '2024-01-10 12:00:00', '{"source": "organic"}')
ON CONFLICT DO NOTHING;

