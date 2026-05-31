-- =============================================================================
-- GenAI Log Analysis POC - Database Schema & Seed Data
-- =============================================================================
-- Tables match the order_no / location_no / timestamp model used throughout
-- the mock log generator. Pre-seeded with 200 orders across 10 locations.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- TABLES
-- -----------------------------------------------------------------------------
CREATE TABLE locations (
    location_no   VARCHAR(10) PRIMARY KEY,
    location_name VARCHAR(100) NOT NULL,
    region        VARCHAR(50) NOT NULL,
    created_at    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE orders (
    order_no       VARCHAR(20) PRIMARY KEY,
    location_no    VARCHAR(10) REFERENCES locations(location_no),
    customer_email VARCHAR(100),
    status         VARCHAR(20) NOT NULL,        -- PENDING, COMPLETED, FAILED, CANCELLED
    failed_step    VARCHAR(30),                  -- NULL unless status=FAILED
    amount         DECIMAL(10,2) NOT NULL,
    currency       VARCHAR(3) DEFAULT 'USD',
    created_at     TIMESTAMP NOT NULL,
    updated_at     TIMESTAMP NOT NULL
);

CREATE INDEX idx_orders_location ON orders(location_no);
CREATE INDEX idx_orders_status   ON orders(status);
CREATE INDEX idx_orders_created  ON orders(created_at);

CREATE TABLE payments (
    payment_id      VARCHAR(20) PRIMARY KEY,
    order_no        VARCHAR(20) REFERENCES orders(order_no),
    status          VARCHAR(20),                 -- SUCCESS, FAILED, PENDING, TIMEOUT
    method          VARCHAR(20),                 -- CARD, UPI, WALLET, NET_BANKING
    failure_reason  VARCHAR(200),                -- populated when status=FAILED
    amount          DECIMAL(10,2),
    attempted_at    TIMESTAMP NOT NULL
);

CREATE INDEX idx_payments_order ON payments(order_no);

CREATE TABLE shipments (
    shipment_id   VARCHAR(20) PRIMARY KEY,
    order_no      VARCHAR(20) REFERENCES orders(order_no),
    status        VARCHAR(20),                  -- PREPARING, SHIPPED, DELIVERED, FAILED
    tracking_id   VARCHAR(50),
    carrier       VARCHAR(50),
    created_at    TIMESTAMP NOT NULL
);

CREATE INDEX idx_shipments_order ON shipments(order_no);

-- -----------------------------------------------------------------------------
-- SEED: LOCATIONS (10 warehouses across regions)
-- -----------------------------------------------------------------------------
INSERT INTO locations (location_no, location_name, region) VALUES
    ('LOC-NYC-01',  'New York Warehouse',    'US-East'),
    ('LOC-LAX-01',  'Los Angeles Warehouse', 'US-West'),
    ('LOC-CHI-01',  'Chicago Warehouse',     'US-Central'),
    ('LOC-LON-01',  'London Warehouse',      'EU-UK'),
    ('LOC-FRA-01',  'Frankfurt Warehouse',   'EU-DE'),
    ('LOC-MUM-01',  'Mumbai Warehouse',      'APAC-IN'),
    ('LOC-BLR-01',  'Bangalore Warehouse',   'APAC-IN'),
    ('LOC-SGP-01',  'Singapore Warehouse',   'APAC-SG'),
    ('LOC-SYD-01',  'Sydney Warehouse',      'APAC-AU'),
    ('LOC-TOR-01',  'Toronto Warehouse',     'CA');

-- -----------------------------------------------------------------------------
-- SEED: ORDERS — 200 orders across the last 14 days
-- Status distribution:
--   60% COMPLETED, 20% FAILED, 15% PENDING, 5% CANCELLED
-- Failed orders include a failed_step for richer RCA scenarios
-- -----------------------------------------------------------------------------
INSERT INTO orders (order_no, location_no, customer_email, status, failed_step, amount, currency, created_at, updated_at)
SELECT
    'ORD-' || LPAD(g::text, 5, '0') as order_no,
    (ARRAY['LOC-NYC-01','LOC-LAX-01','LOC-CHI-01','LOC-LON-01','LOC-FRA-01',
           'LOC-MUM-01','LOC-BLR-01','LOC-SGP-01','LOC-SYD-01','LOC-TOR-01'])[1 + (g % 10)] as location_no,
    'customer' || g || '@example.com' as customer_email,
    CASE
        WHEN g % 20 = 0 THEN 'CANCELLED'
        WHEN g % 5 = 0  THEN 'FAILED'
        WHEN g % 7 = 0  THEN 'PENDING'
        ELSE 'COMPLETED'
    END as status,
    CASE
        WHEN g % 5 = 0 AND g % 20 != 0 THEN
            (ARRAY['PAYMENT','INVENTORY','SHIPPING','FRAUD_CHECK','DB_TIMEOUT'])[1 + (g % 5)]
        ELSE NULL
    END as failed_step,
    ROUND((50 + (g % 950))::numeric, 2) as amount,
    'USD' as currency,
    NOW() - (g || ' minutes')::interval as created_at,
    NOW() - (g || ' minutes')::interval + INTERVAL '5 minutes' as updated_at
FROM generate_series(1, 200) g;

-- -----------------------------------------------------------------------------
-- SEED: PAYMENTS — one row per order, mirroring order outcome
-- Includes various failure reasons matching the failed_step taxonomy
-- -----------------------------------------------------------------------------
INSERT INTO payments (payment_id, order_no, status, method, failure_reason, amount, attempted_at)
SELECT
    'PAY-' || LPAD(g::text, 5, '0') as payment_id,
    'ORD-' || LPAD(g::text, 5, '0') as order_no,
    CASE
        WHEN g % 20 = 0 THEN 'PENDING'
        WHEN g % 5 = 0 AND g % 20 != 0 AND g % 5 = 0 THEN
            CASE
                WHEN (g % 25) IN (5, 10) THEN 'TIMEOUT'
                ELSE 'FAILED'
            END
        WHEN g % 7 = 0 THEN 'PENDING'
        ELSE 'SUCCESS'
    END as status,
    (ARRAY['CARD','UPI','WALLET','NET_BANKING'])[1 + (g % 4)] as method,
    CASE
        WHEN g % 5 = 0 AND g % 20 != 0 THEN
            (ARRAY[
                'PSP gateway timeout after 5000ms',
                'Card declined by issuer (insufficient funds)',
                'Database connection pool exhausted',
                'NullPointerException in PaymentValidator.validate()',
                'Circuit breaker OPEN for payment-service',
                'CVV verification failed',
                '3D Secure authentication abandoned by user',
                'Connection refused to PSP-Stripe at 14:32 UTC'
            ])[1 + (g % 8)]
        ELSE NULL
    END as failure_reason,
    ROUND((50 + (g % 950))::numeric, 2) as amount,
    NOW() - (g || ' minutes')::interval + INTERVAL '1 minute' as attempted_at
FROM generate_series(1, 200) g;

-- -----------------------------------------------------------------------------
-- SEED: SHIPMENTS — only for COMPLETED orders
-- -----------------------------------------------------------------------------
INSERT INTO shipments (shipment_id, order_no, status, tracking_id, carrier, created_at)
SELECT
    'SHP-' || LPAD(g::text, 5, '0') as shipment_id,
    'ORD-' || LPAD(g::text, 5, '0') as order_no,
    CASE
        WHEN g % 3 = 0 THEN 'DELIVERED'
        WHEN g % 4 = 0 THEN 'SHIPPED'
        ELSE 'PREPARING'
    END as status,
    'TRK' || (1000000 + g) as tracking_id,
    (ARRAY['FedEx','UPS','DHL','BlueDart'])[1 + (g % 4)] as carrier,
    NOW() - (g || ' minutes')::interval + INTERVAL '10 minutes' as created_at
FROM generate_series(1, 200) g
WHERE g % 5 != 0 AND g % 7 != 0 AND g % 20 != 0;  -- only successful orders

-- -----------------------------------------------------------------------------
-- VERIFICATION QUERIES (run these to sanity-check the data)
-- -----------------------------------------------------------------------------
-- SELECT status, COUNT(*) FROM orders GROUP BY status;
-- SELECT failed_step, COUNT(*) FROM orders WHERE status='FAILED' GROUP BY failed_step;
-- SELECT * FROM orders WHERE status='FAILED' LIMIT 10;
-- SELECT o.order_no, o.status, o.failed_step, p.status as pay_status, p.failure_reason
--   FROM orders o JOIN payments p ON p.order_no = o.order_no
--   WHERE o.status = 'FAILED' LIMIT 10;

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO postgres;
