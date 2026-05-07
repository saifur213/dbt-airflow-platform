-- RUN this command to setup source databse
-- sudo -u postgres psql -f db/postgres/prepare_source_db.sql

-- Create User
DO $$
BEGIN
   IF NOT EXISTS (
      SELECT FROM pg_roles WHERE rolname = 'etl_user'
   ) THEN
      CREATE USER etl_user WITH PASSWORD 'etluser26';
   END IF;
END
$$;

-- Create Database
SELECT 'CREATE DATABASE source_db'
WHERE NOT EXISTS (
    SELECT FROM pg_database WHERE datname = 'source_db'
)\gexec

-- grant permission
GRANT CONNECT ON DATABASE source_db TO etl_user;

\c source_db

GRANT USAGE ON SCHEMA public TO etl_user;

GRANT SELECT, INSERT, UPDATE, DELETE
ON ALL TABLES IN SCHEMA public
TO etl_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO etl_user;

-- ═══════════════════════════════════════════════════════════════════════════
-- SOURCE DATABASE SETUP — DDL + SAMPLE DATA
-- Database : source_db
-- Schema   : public
-- Purpose  : Airflow → dbt → Snowflake pipeline source
-- Run as   : superuser (postgres) for setup; etl_user can only SELECT
-- ═══════════════════════════════════════════════════════════════════════════

-- ─────────────────────────────────────────────────────────────────────────────
-- EXTENSIONS
-- ─────────────────────────────────────────────────────────────────────────────

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";      -- uuid_generate_v4()
CREATE EXTENSION IF NOT EXISTS "pgcrypto";       -- gen_random_uuid() (Postgres 13+)


-- ═══════════════════════════════════════════════════════════════════════════
-- SECTION 1 — CREATE TABLES
-- ═══════════════════════════════════════════════════════════════════════════

-- ─────────────────────────────────────────────────────────────────────────────
-- 1.1  customers
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS public.customers (
    customer_id         bigserial                   NOT NULL,
    external_id         uuid                        NOT NULL DEFAULT gen_random_uuid(),
    email               varchar(255)                NOT NULL,
    full_name           varchar(255)                NOT NULL,
    phone               varchar(50),
    segment             varchar(50),                -- 'enterprise', 'smb', 'consumer'
    country_code        char(2),
    city                varchar(100),
    is_active           boolean                     NOT NULL DEFAULT true,
    created_at          timestamp with time zone    NOT NULL DEFAULT now(),
    updated_at          timestamp with time zone    NOT NULL DEFAULT now(),

    CONSTRAINT customers_pkey            PRIMARY KEY (customer_id),
    CONSTRAINT customers_email_unique    UNIQUE      (email),
    CONSTRAINT customers_ext_id_unique   UNIQUE      (external_id),
    CONSTRAINT customers_segment_check   CHECK       (segment IN ('enterprise','smb','consumer'))
);

COMMENT ON TABLE  public.customers                IS 'End customers — source of truth for CRM data';
COMMENT ON COLUMN public.customers.external_id    IS 'UUID exposed to external systems (never expose customer_id)';
COMMENT ON COLUMN public.customers.segment        IS 'Business segment used for dbt mart partitioning';
COMMENT ON COLUMN public.customers.updated_at     IS 'ETL watermark column — index maintained for incremental extract';


-- ─────────────────────────────────────────────────────────────────────────────
-- 1.2  products
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS public.products (
    product_id          bigserial                   NOT NULL,
    sku                 varchar(100)                NOT NULL,
    name                varchar(255)                NOT NULL,
    description         text,
    category            varchar(100),
    sub_category        varchar(100),
    unit_price          numeric(18,4)               NOT NULL,
    cost_price          numeric(18,4),
    currency            char(3)                     NOT NULL DEFAULT 'USD',
    weight_kg           numeric(8,3),
    is_active           boolean                     NOT NULL DEFAULT true,
    created_at          timestamp with time zone    NOT NULL DEFAULT now(),
    updated_at          timestamp with time zone    NOT NULL DEFAULT now(),

    CONSTRAINT products_pkey        PRIMARY KEY (product_id),
    CONSTRAINT products_sku_unique  UNIQUE      (sku),
    CONSTRAINT products_price_check CHECK       (unit_price >= 0),
    CONSTRAINT products_cost_check  CHECK       (cost_price IS NULL OR cost_price >= 0)
);

COMMENT ON TABLE  public.products               IS 'Product catalogue — source of truth for all sellable items';
COMMENT ON COLUMN public.products.updated_at    IS 'ETL watermark column';


-- ─────────────────────────────────────────────────────────────────────────────
-- 1.3  orders
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS public.orders (
    order_id            bigserial                   NOT NULL,
    order_ref           varchar(50)                 NOT NULL,   -- human-readable ref e.g. ORD-000123
    customer_id         bigint                      NOT NULL,
    status              varchar(50)                 NOT NULL DEFAULT 'pending',
    order_total         numeric(18,2)               NOT NULL DEFAULT 0,
    tax_amount          numeric(18,2)               NOT NULL DEFAULT 0,
    discount_amount     numeric(18,2)               NOT NULL DEFAULT 0,
    shipping_amount     numeric(18,2)               NOT NULL DEFAULT 0,
    currency            char(3)                     NOT NULL DEFAULT 'USD',
    shipping_country    char(2),
    notes               text,
    placed_at           timestamp with time zone,
    shipped_at          timestamp with time zone,
    delivered_at        timestamp with time zone,
    cancelled_at        timestamp with time zone,
    created_at          timestamp with time zone    NOT NULL DEFAULT now(),
    updated_at          timestamp with time zone    NOT NULL DEFAULT now(),

    CONSTRAINT orders_pkey              PRIMARY KEY (order_id),
    CONSTRAINT orders_ref_unique        UNIQUE      (order_ref),
    CONSTRAINT orders_customer_fk       FOREIGN KEY (customer_id) REFERENCES public.customers(customer_id),
    CONSTRAINT orders_status_check      CHECK       (status IN ('pending','confirmed','processing','shipped','delivered','cancelled','refunded')),
    CONSTRAINT orders_total_check       CHECK       (order_total >= 0),
    CONSTRAINT orders_tax_check         CHECK       (tax_amount  >= 0),
    CONSTRAINT orders_discount_check    CHECK       (discount_amount >= 0)
);

COMMENT ON TABLE  public.orders              IS 'Transactional orders — append-heavy, updated on status transitions';
COMMENT ON COLUMN public.orders.order_ref    IS 'Human-readable reference shown to customer';
COMMENT ON COLUMN public.orders.updated_at   IS 'ETL watermark column — updated on every status change';


-- ─────────────────────────────────────────────────────────────────────────────
-- 1.4  order_items
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS public.order_items (
    item_id             bigserial                   NOT NULL,
    order_id            bigint                      NOT NULL,
    product_id          bigint                      NOT NULL,
    quantity            integer                     NOT NULL DEFAULT 1,
    unit_price          numeric(18,4)               NOT NULL,   -- price at time of order (snapshot)
    discount_pct        numeric(5,2)                NOT NULL DEFAULT 0,
    line_total          numeric(18,2)               NOT NULL,   -- quantity * unit_price * (1 - discount_pct/100)
    created_at          timestamp with time zone    NOT NULL DEFAULT now(),

    CONSTRAINT order_items_pkey         PRIMARY KEY (item_id),
    CONSTRAINT order_items_order_fk     FOREIGN KEY (order_id)   REFERENCES public.orders(order_id),
    CONSTRAINT order_items_product_fk   FOREIGN KEY (product_id) REFERENCES public.products(product_id),
    CONSTRAINT order_items_qty_check    CHECK       (quantity > 0),
    CONSTRAINT order_items_price_check  CHECK       (unit_price >= 0),
    CONSTRAINT order_items_disc_check   CHECK       (discount_pct BETWEEN 0 AND 100)
);

COMMENT ON TABLE  public.order_items                IS 'Line items — one row per product per order; immutable after insert';
COMMENT ON COLUMN public.order_items.unit_price     IS 'Snapshot of product price at order time — do not join to products for historical pricing';
COMMENT ON COLUMN public.order_items.line_total     IS 'Denormalised for read performance; recomputed on insert';


-- ─────────────────────────────────────────────────────────────────────────────
-- 1.5  payments
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS public.payments (
    payment_id          bigserial                   NOT NULL,
    order_id            bigint                      NOT NULL,
    payment_method      varchar(50)                 NOT NULL,   -- 'card','bank_transfer','wallet','cash'
    payment_provider    varchar(100),                           -- 'stripe','paypal','adyen'
    provider_ref        varchar(255),                           -- provider transaction ID
    amount              numeric(18,2)               NOT NULL,
    currency            char(3)                     NOT NULL DEFAULT 'USD',
    status              varchar(50)                 NOT NULL DEFAULT 'pending',
    failure_reason      text,
    captured_at         timestamp with time zone,
    refunded_at         timestamp with time zone,
    created_at          timestamp with time zone    NOT NULL DEFAULT now(),
    updated_at          timestamp with time zone    NOT NULL DEFAULT now(),

    CONSTRAINT payments_pkey            PRIMARY KEY (payment_id),
    CONSTRAINT payments_order_fk        FOREIGN KEY (order_id) REFERENCES public.orders(order_id),
    CONSTRAINT payments_method_check    CHECK       (payment_method IN ('card','bank_transfer','wallet','cash')),
    CONSTRAINT payments_status_check    CHECK       (status IN ('pending','authorised','captured','failed','refunded','disputed')),
    CONSTRAINT payments_amount_check    CHECK       (amount > 0)
);

COMMENT ON TABLE  public.payments               IS 'Payment transactions — one or more per order (supports split payments)';
COMMENT ON COLUMN public.payments.updated_at    IS 'ETL watermark column';


-- ═══════════════════════════════════════════════════════════════════════════
-- SECTION 2 — INDEXES (all CONCURRENTLY — run outside a transaction block)
-- ═══════════════════════════════════════════════════════════════════════════

-- customers
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_customers_updated_at  ON public.customers (updated_at);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_customers_email       ON public.customers (email);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_customers_segment     ON public.customers (segment) WHERE segment IS NOT NULL;

-- products
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_products_updated_at   ON public.products (updated_at);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_products_category     ON public.products (category) WHERE category IS NOT NULL;
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_products_sku          ON public.products (sku);

-- orders — composite on watermark + pk for efficient incremental scans
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_orders_updated_at     ON public.orders (updated_at, order_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_orders_customer_id    ON public.orders (customer_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_orders_status         ON public.orders (status);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_orders_placed_at      ON public.orders (placed_at) WHERE placed_at IS NOT NULL;

-- order_items — created_at watermark + FK lookups
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_order_items_order_id    ON public.order_items (order_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_order_items_product_id  ON public.order_items (product_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_order_items_created_at  ON public.order_items (created_at, item_id);

-- payments
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_payments_updated_at   ON public.payments (updated_at);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_payments_order_id     ON public.payments (order_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_payments_status       ON public.payments (status);


-- ═══════════════════════════════════════════════════════════════════════════
-- SECTION 3 — UPDATED_AT TRIGGER
-- Auto-maintain updated_at on every UPDATE — ETL relies on this for watermarks
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION public.set_updated_at()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$;

DO $$ DECLARE
    t text;
BEGIN
    FOREACH t IN ARRAY ARRAY['customers','products','orders','payments'] LOOP
        EXECUTE format('
            DROP TRIGGER IF EXISTS trg_%1$s_updated_at ON public.%1$s;
            CREATE TRIGGER trg_%1$s_updated_at
                BEFORE UPDATE ON public.%1$s
                FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();
        ', t);
    END LOOP;
END $$;


-- ═══════════════════════════════════════════════════════════════════════════
-- SECTION 4 — SAMPLE DATA
-- 10 customers · 15 products · 25 orders · ~60 order_items · 28 payments
-- Data spans the last 90 days — simulates realistic ETL watermark ranges
-- ═══════════════════════════════════════════════════════════════════════════

-- ─────────────────────────────────────────────────────────────────────────────
-- 4.1  customers (10 rows)
-- ─────────────────────────────────────────────────────────────────────────────

INSERT INTO public.customers
    (email, full_name, phone, segment, country_code, city, is_active, created_at, updated_at)
VALUES
    ('alice.johnson@acmecorp.com',    'Alice Johnson',    '+1-415-555-0101', 'enterprise', 'US', 'San Francisco', true,  now() - interval '88 days', now() - interval '2 days'),
    ('bob.smith@techwave.io',         'Bob Smith',        '+1-212-555-0182', 'smb',        'US', 'New York',      true,  now() - interval '75 days', now() - interval '5 days'),
    ('carol.white@sunrise.de',        'Carol White',      '+49-30-555-0193', 'enterprise', 'DE', 'Berlin',        true,  now() - interval '60 days', now() - interval '1 day'),
    ('david.lee@fastmail.jp',         'David Lee',        '+81-3-555-0174',  'consumer',   'JP', 'Tokyo',         true,  now() - interval '55 days', now() - interval '10 days'),
    ('emma.brown@globalink.co.uk',    'Emma Brown',       '+44-20-555-0165', 'smb',        'GB', 'London',        true,  now() - interval '48 days', now() - interval '3 days'),
    ('frank.garcia@cloudbase.es',     'Frank Garcia',     '+34-91-555-0126', 'consumer',   'ES', 'Madrid',        true,  now() - interval '40 days', now() - interval '7 days'),
    ('grace.kim@nexustech.kr',        'Grace Kim',        '+82-2-555-0137',  'enterprise', 'KR', 'Seoul',         true,  now() - interval '33 days', now() - interval '1 day'),
    ('henry.zhang@datasync.cn',       'Henry Zhang',      '+86-21-555-0118', 'smb',        'CN', 'Shanghai',      true,  now() - interval '25 days', now() - interval '4 days'),
    ('isabella.rossi@primotech.it',   'Isabella Rossi',   '+39-06-555-0149', 'consumer',   'IT', 'Rome',          true,  now() - interval '15 days', now() - interval '2 days'),
    ('james.wilson@inactiveuser.ca',  'James Wilson',     '+1-604-555-0110', 'smb',        'CA', 'Vancouver',     false, now() - interval '90 days', now() - interval '60 days')
ON CONFLICT (email) DO NOTHING;


-- ─────────────────────────────────────────────────────────────────────────────
-- 4.2  products (15 rows)
-- ─────────────────────────────────────────────────────────────────────────────

INSERT INTO public.products
    (sku, name, description, category, sub_category, unit_price, cost_price, currency, weight_kg, is_active, created_at, updated_at)
VALUES
    ('SOFT-ANLYT-001', 'Analytics Pro',        'BI analytics suite — annual licence',   'Software',  'Analytics',   1200.00,  NULL,   'USD', NULL,  true,  now() - interval '200 days', now() - interval '30 days'),
    ('SOFT-ANLYT-002', 'Analytics Starter',    'BI analytics suite — monthly licence',  'Software',  'Analytics',    149.00,  NULL,   'USD', NULL,  true,  now() - interval '200 days', now() - interval '30 days'),
    ('SOFT-ETL-001',   'DataBridge ETL',       'No-code ETL pipeline builder',          'Software',  'Integration', 2400.00,  NULL,   'USD', NULL,  true,  now() - interval '150 days', now() - interval '10 days'),
    ('SOFT-ML-001',    'ML Studio',            'AutoML model training platform',        'Software',  'AI/ML',       3600.00,  NULL,   'USD', NULL,  true,  now() - interval '120 days', now() - interval '5 days'),
    ('HW-LAPTOP-001',  'ProBook 15',           '15" developer laptop, 32GB RAM',        'Hardware',  'Laptops',     1899.99,  980.00, 'USD', 1.85,  true,  now() - interval '300 days', now() - interval '20 days'),
    ('HW-LAPTOP-002',  'UltraSlim 13',         '13" ultrabook, 16GB RAM',               'Hardware',  'Laptops',     1199.99,  620.00, 'USD', 1.10,  true,  now() - interval '300 days', now() - interval '20 days'),
    ('HW-MON-001',     '4K Monitor 27"',       '27" 4K IPS display 144Hz',             'Hardware',  'Monitors',     699.99,  320.00, 'USD', 6.20,  true,  now() - interval '250 days', now() - interval '15 days'),
    ('HW-DOCK-001',    'USB-C Docking Station','12-port USB-C hub with 4K support',     'Hardware',  'Accessories',  199.99,   85.00, 'USD', 0.35,  true,  now() - interval '180 days', now() - interval '8 days'),
    ('SVC-ONBOARD-001','Onboarding Package',   'Dedicated onboarding — 5 sessions',     'Services',  'Onboarding',   999.00,  NULL,   'USD', NULL,  true,  now() - interval '100 days', now() - interval '3 days'),
    ('SVC-SUPPORT-001','Support Gold',         '24/7 priority support — annual',        'Services',  'Support',     1800.00,  NULL,   'USD', NULL,  true,  now() - interval '100 days', now() - interval '3 days'),
    ('SVC-TRAIN-001',  'Training Bootcamp',    '3-day in-person training workshop',     'Services',  'Training',    2500.00,  NULL,   'USD', NULL,  true,  now() - interval '80 days',  now() - interval '1 day'),
    ('HW-LAPTOP-003',  'DevStation Pro',       '16" powerhouse, 64GB RAM, RTX 4080',   'Hardware',  'Laptops',     3499.99, 1800.00,'USD', 2.40,  true,  now() - interval '60 days',  now() - interval '5 days'),
    ('SOFT-SEC-001',   'SecureVault',          'End-to-end encryption SDK',             'Software',  'Security',     899.00,  NULL,   'USD', NULL,  true,  now() - interval '45 days',  now() - interval '2 days'),
    ('HW-HEADSET-001', 'NoiseCancel Pro',      'ANC wireless headset for remote teams', 'Hardware',  'Accessories',  349.99,  140.00, 'USD', 0.28,  true,  now() - interval '30 days',  now() - interval '1 day'),
    ('SOFT-COLLAB-001','TeamFlow',             'Async collaboration & wiki platform',   'Software',  'Productivity', 599.00,  NULL,   'USD', NULL,  false, now() - interval '20 days',  now() - interval '1 day')
ON CONFLICT (sku) DO NOTHING;


-- ─────────────────────────────────────────────────────────────────────────────
-- 4.3  orders (25 rows) — various statuses, spanning 90 days
-- ─────────────────────────────────────────────────────────────────────────────

INSERT INTO public.orders
    (order_ref, customer_id, status, order_total, tax_amount, discount_amount,
     shipping_amount, currency, shipping_country, placed_at, shipped_at, delivered_at,
     cancelled_at, created_at, updated_at)
SELECT
    o.order_ref, c.customer_id, o.status,
    o.order_total, o.tax_amount, o.discount_amount, o.shipping_amount,
    o.currency, o.shipping_country,
    o.placed_at, o.shipped_at, o.delivered_at, o.cancelled_at,
    o.created_at, o.updated_at
FROM (VALUES
    -- (order_ref, email, status, total, tax, disc, ship, currency, country, placed, shipped, delivered, cancelled, created, updated)
    ('ORD-000001','alice.johnson@acmecorp.com',   'delivered',  2399.99, 192.00,   0.00, 15.00,'USD','US', now()-'85d'::interval, now()-'83d'::interval, now()-'80d'::interval, NULL,            now()-'85d'::interval, now()-'80d'::interval),
    ('ORD-000002','bob.smith@techwave.io',         'delivered',  1349.99, 108.00,   0.00, 15.00,'USD','US', now()-'72d'::interval, now()-'70d'::interval, now()-'67d'::interval, NULL,            now()-'72d'::interval, now()-'67d'::interval),
    ('ORD-000003','carol.white@sunrise.de',        'delivered',  3600.00, 684.00,   0.00,  0.00,'USD','DE', now()-'58d'::interval, now()-'56d'::interval, now()-'53d'::interval, NULL,            now()-'58d'::interval, now()-'53d'::interval),
    ('ORD-000004','david.lee@fastmail.jp',         'delivered',   699.99,  63.00,   0.00, 20.00,'USD','JP', now()-'52d'::interval, now()-'50d'::interval, now()-'47d'::interval, NULL,            now()-'52d'::interval, now()-'47d'::interval),
    ('ORD-000005','emma.brown@globalink.co.uk',    'delivered',  4299.99, 344.00, 200.00, 15.00,'USD','GB', now()-'45d'::interval, now()-'43d'::interval, now()-'40d'::interval, NULL,            now()-'45d'::interval, now()-'40d'::interval),
    ('ORD-000006','frank.garcia@cloudbase.es',     'cancelled',   349.99,  31.50,   0.00,  0.00,'USD','ES', now()-'42d'::interval, NULL,                  NULL,                  now()-'41d'::interval, now()-'42d'::interval, now()-'41d'::interval),
    ('ORD-000007','grace.kim@nexustech.kr',        'delivered',  2200.00, 198.00,   0.00,  0.00,'USD','KR', now()-'30d'::interval, now()-'28d'::interval, now()-'25d'::interval, NULL,            now()-'30d'::interval, now()-'25d'::interval),
    ('ORD-000008','henry.zhang@datasync.cn',       'shipped',    1899.99, 152.00,   0.00, 25.00,'USD','CN', now()-'20d'::interval, now()-'18d'::interval, NULL,                  NULL,            now()-'20d'::interval, now()-'18d'::interval),
    ('ORD-000009','isabella.rossi@primotech.it',   'processing',  999.00,  89.91,   0.00,  0.00,'USD','IT', now()-'12d'::interval, NULL,                  NULL,                  NULL,            now()-'12d'::interval, now()-'12d'::interval),
    ('ORD-000010','alice.johnson@acmecorp.com',    'delivered',  5499.99, 440.00, 500.00,  0.00,'USD','US', now()-'40d'::interval, now()-'38d'::interval, now()-'35d'::interval, NULL,            now()-'40d'::interval, now()-'35d'::interval),
    ('ORD-000011','bob.smith@techwave.io',         'confirmed',  1200.00, 108.00,   0.00,  0.00,'USD','US', now()-'8d'::interval,  NULL,                  NULL,                  NULL,            now()-'8d'::interval,  now()-'8d'::interval),
    ('ORD-000012','carol.white@sunrise.de',        'delivered',  2899.99, 551.00,   0.00, 20.00,'USD','DE', now()-'35d'::interval, now()-'33d'::interval, now()-'30d'::interval, NULL,            now()-'35d'::interval, now()-'30d'::interval),
    ('ORD-000013','grace.kim@nexustech.kr',        'delivered',  4799.99, 432.00, 300.00,  0.00,'USD','KR', now()-'28d'::interval, now()-'26d'::interval, now()-'23d'::interval, NULL,            now()-'28d'::interval, now()-'23d'::interval),
    ('ORD-000014','emma.brown@globalink.co.uk',    'refunded',    699.99,  56.00,   0.00, 15.00,'USD','GB', now()-'50d'::interval, now()-'48d'::interval, now()-'45d'::interval, NULL,            now()-'50d'::interval, now()-'22d'::interval),
    ('ORD-000015','david.lee@fastmail.jp',         'pending',     199.99,  18.00,   0.00,  8.00,'USD','JP', now()-'1d'::interval,  NULL,                  NULL,                  NULL,            now()-'1d'::interval,  now()-'1d'::interval),
    ('ORD-000016','henry.zhang@datasync.cn',       'delivered',  3499.99, 280.00,   0.00, 25.00,'USD','CN', now()-'22d'::interval, now()-'20d'::interval, now()-'17d'::interval, NULL,            now()-'22d'::interval, now()-'17d'::interval),
    ('ORD-000017','isabella.rossi@primotech.it',   'confirmed',   349.99,  31.50,   0.00,  0.00,'USD','IT', now()-'3d'::interval,  NULL,                  NULL,                  NULL,            now()-'3d'::interval,  now()-'3d'::interval),
    ('ORD-000018','alice.johnson@acmecorp.com',    'processing', 2400.00, 192.00,   0.00,  0.00,'USD','US', now()-'5d'::interval,  NULL,                  NULL,                  NULL,            now()-'5d'::interval,  now()-'5d'::interval),
    ('ORD-000019','frank.garcia@cloudbase.es',     'shipped',     899.00,  80.91,   0.00,  0.00,'USD','ES', now()-'10d'::interval, now()-'8d'::interval,  NULL,                  NULL,            now()-'10d'::interval, now()-'8d'::interval),
    ('ORD-000020','bob.smith@techwave.io',         'delivered',  1800.00, 162.00,   0.00,  0.00,'USD','US', now()-'18d'::interval, now()-'16d'::interval, now()-'13d'::interval, NULL,            now()-'18d'::interval, now()-'13d'::interval),
    ('ORD-000021','grace.kim@nexustech.kr',        'confirmed',  3600.00, 324.00,   0.00,  0.00,'USD','KR', now()-'2d'::interval,  NULL,                  NULL,                  NULL,            now()-'2d'::interval,  now()-'2d'::interval),
    ('ORD-000022','carol.white@sunrise.de',        'pending',    1199.99,  96.00,   0.00, 20.00,'USD','DE', now()-interval'6 hours',NULL,                  NULL,                  NULL,            now()-interval'6 hours',now()-interval'6 hours'),
    ('ORD-000023','henry.zhang@datasync.cn',       'delivered',   599.00,  47.92,  60.00,  0.00,'USD','CN', now()-'14d'::interval, now()-'12d'::interval, now()-'9d'::interval,  NULL,            now()-'14d'::interval, now()-'9d'::interval),
    ('ORD-000024','emma.brown@globalink.co.uk',    'cancelled',  2500.00, 200.00,   0.00,  0.00,'USD','GB', now()-'6d'::interval,  NULL,                  NULL,                  now()-'5d'::interval, now()-'6d'::interval, now()-'5d'::interval),
    ('ORD-000025','david.lee@fastmail.jp',         'shipped',     549.98,  49.50,   0.00, 10.00,'USD','JP', now()-'4d'::interval,  now()-'2d'::interval,  NULL,                  NULL,            now()-'4d'::interval,  now()-'2d'::interval)
) AS o(order_ref, email, status, order_total, tax_amount, discount_amount, shipping_amount,
       currency, shipping_country, placed_at, shipped_at, delivered_at, cancelled_at,
       created_at, updated_at)
JOIN public.customers c ON c.email = o.email
ON CONFLICT (order_ref) DO NOTHING;


-- ─────────────────────────────────────────────────────────────────────────────
-- 4.4  order_items (~60 rows, 2-3 items per order)
-- ─────────────────────────────────────────────────────────────────────────────

INSERT INTO public.order_items
    (order_id, product_id, quantity, unit_price, discount_pct, line_total, created_at)
SELECT
    o.order_id,
    p.product_id,
    i.quantity,
    p.unit_price,
    i.discount_pct,
    ROUND((i.quantity * p.unit_price * (1 - i.discount_pct / 100))::numeric, 2),
    o.created_at
FROM (VALUES
    --  (order_ref,      sku,               qty, disc_pct)
    ('ORD-000001', 'SOFT-ANLYT-001',          1,  0),
    ('ORD-000001', 'HW-DOCK-001',             1,  0),
    ('ORD-000002', 'HW-LAPTOP-002',           1,  0),
    ('ORD-000003', 'SOFT-ML-001',             1,  0),
    ('ORD-000004', 'HW-MON-001',              1,  0),
    ('ORD-000005', 'HW-LAPTOP-001',           2,  5),
    ('ORD-000005', 'HW-DOCK-001',             2,  0),
    ('ORD-000006', 'HW-HEADSET-001',          1,  0),
    ('ORD-000007', 'SOFT-ETL-001',            1,  0),
    ('ORD-000007', 'SVC-ONBOARD-001',         1,  0),
    ('ORD-000008', 'HW-LAPTOP-001',           1,  0),
    ('ORD-000009', 'SVC-ONBOARD-001',         1,  0),
    ('ORD-000010', 'HW-LAPTOP-003',           1, 10),
    ('ORD-000010', 'HW-MON-001',              1,  5),
    ('ORD-000010', 'HW-HEADSET-001',          1,  5),
    ('ORD-000011', 'SOFT-ANLYT-001',          1,  0),
    ('ORD-000012', 'HW-LAPTOP-002',           1,  0),
    ('ORD-000012', 'SVC-SUPPORT-001',         1,  0),
    ('ORD-000013', 'SOFT-ML-001',             1,  0),
    ('ORD-000013', 'SVC-TRAIN-001',           1,  0),
    ('ORD-000013', 'HW-DOCK-001',             2,  0),
    ('ORD-000014', 'HW-MON-001',              1,  0),
    ('ORD-000015', 'HW-DOCK-001',             1,  0),
    ('ORD-000016', 'HW-LAPTOP-003',           1,  0),
    ('ORD-000017', 'HW-HEADSET-001',          1,  0),
    ('ORD-000018', 'SOFT-ETL-001',            1,  0),
    ('ORD-000019', 'SOFT-SEC-001',            1,  0),
    ('ORD-000020', 'SVC-SUPPORT-001',         1,  0),
    ('ORD-000021', 'SOFT-ML-001',             1,  0),
    ('ORD-000022', 'HW-LAPTOP-002',           1,  0),
    ('ORD-000023', 'SOFT-COLLAB-001',         1, 10),
    ('ORD-000024', 'SVC-TRAIN-001',           1,  0),
    ('ORD-000025', 'SOFT-ANLYT-002',          2,  0),
    ('ORD-000025', 'HW-HEADSET-001',          1,  0)
) AS i(order_ref, sku, quantity, discount_pct)
JOIN public.orders  o ON o.order_ref  = i.order_ref
JOIN public.products p ON p.sku       = i.sku;


-- ─────────────────────────────────────────────────────────────────────────────
-- 4.5  payments (28 rows — most orders paid, some pending/failed)
-- ─────────────────────────────────────────────────────────────────────────────
INSERT INTO public.payments
    (order_id, payment_method, payment_provider, provider_ref, amount, currency,
     status, failure_reason, captured_at, created_at, updated_at)
SELECT
    o.order_id,
    p.payment_method,
    p.payment_provider,
    p.provider_ref,
    o.order_total,
    o.currency,
    p.status,
    p.failure_reason,
    p.captured_at,
    o.created_at,
    p.updated_at
FROM (
VALUES
    ('ORD-000001','card',          'stripe',  'ch_1A2B3C4D5E', 'captured', NULL,          now()-'85d'::interval, now()-'85d'::interval),
    ('ORD-000002','card',          'stripe',  'ch_2B3C4D5E6F', 'captured', NULL,          now()-'72d'::interval, now()-'72d'::interval),
    ('ORD-000003','bank_transfer', 'adyen',   'AT_3C4D5E6F7G', 'captured', NULL,          now()-'58d'::interval, now()-'58d'::interval),
    ('ORD-000004','wallet',        'paypal',  'PP_4D5E6F7G8H', 'captured', NULL,          now()-'52d'::interval, now()-'52d'::interval),
    ('ORD-000005','card',          'stripe',  'ch_5E6F7G8H9I', 'captured', NULL,          now()-'45d'::interval, now()-'45d'::interval),
    ('ORD-000006','card',          'stripe',  'ch_6F7G8H9I0J', 'refunded', NULL,          now()-'42d'::interval, now()-'41d'::interval),
    ('ORD-000007','bank_transfer', 'adyen',   'AT_7G8H9I0J1K', 'captured', NULL,          now()-'30d'::interval, now()-'30d'::interval),
    ('ORD-000008','card',          'stripe',  'ch_8H9I0J1K2L', 'captured', NULL,          now()-'20d'::interval, now()-'20d'::interval),
    ('ORD-000009','card',          'stripe',  'ch_9I0J1K2L3M', 'authorised',NULL,         now()-'12d'::interval, now()-'12d'::interval),
    ('ORD-000010','card',          'stripe',  'ch_0J1K2L3M4N', 'captured', NULL,          now()-'40d'::interval, now()-'40d'::interval),
    ('ORD-000011','bank_transfer', 'adyen',   'AT_1K2L3M4N5O', 'authorised',NULL,         NULL,                  now()-'8d'::interval),
    ('ORD-000012','card',          'stripe',  'ch_2L3M4N5O6P', 'captured', NULL,          now()-'35d'::interval, now()-'35d'::interval),
    ('ORD-000013','wallet',        'paypal',  'PP_3M4N5O6P7Q', 'captured', NULL,          now()-'28d'::interval, now()-'28d'::interval),
    ('ORD-000014','card',          'stripe',  'ch_4N5O6P7Q8R', 'refunded', NULL,          now()-'50d'::interval, now()-'22d'::interval),
    ('ORD-000015','card',          'stripe',  'ch_5O6P7Q8R9S', 'pending',  NULL,          NULL,                  now()-'1d'::interval),
    ('ORD-000016','bank_transfer', 'adyen',   'AT_6P7Q8R9S0T', 'captured', NULL,          now()-'22d'::interval, now()-'22d'::interval),
    ('ORD-000017','card',          'stripe',  'ch_7Q8R9S0T1U', 'authorised',NULL,         NULL,                  now()-'3d'::interval),
    ('ORD-000018','card',          'stripe',  'ch_8R9S0T1U2V', 'authorised',NULL,         NULL,                  now()-'5d'::interval),
    ('ORD-000019','wallet',        'paypal',  'PP_9S0T1U2V3W', 'captured', NULL,          now()-'10d'::interval, now()-'10d'::interval),
    ('ORD-000020','card',          'stripe',  'ch_0T1U2V3W4X', 'captured', NULL,          now()-'18d'::interval, now()-'18d'::interval),
    ('ORD-000021','bank_transfer', 'adyen',   'AT_1U2V3W4X5Y', 'authorised',NULL,         NULL,                  now()-'2d'::interval),
    ('ORD-000022','card',          'stripe',  'ch_2V3W4X5Y6Z', 'pending',  NULL,          NULL,                  now()-interval'6 hours'),
    ('ORD-000023','card',          'stripe',  'ch_3W4X5Y6Z7A', 'captured', NULL,          now()-'14d'::interval, now()-'14d'::interval),
    ('ORD-000024','card',          'stripe',  'ch_4X5Y6Z7A8B', 'refunded', NULL,          now()-'6d'::interval,  now()-'5d'::interval),
    ('ORD-000025','wallet',        'paypal',  'PP_5Y6Z7A8B9C', 'captured', NULL,          now()-'4d'::interval,  now()-'4d'::interval),
    -- Failed payment then retry (ORD-000009 had a failed card first)
    ('ORD-000009','card',          'stripe',  'ch_FAIL_001',   'failed',  'insufficient_funds', NULL,            now()-'13d'::interval),
    -- Disputed payment
    ('ORD-000005','card',          'stripe',  'ch_DISP_001',  'disputed', 'chargeback filed',   now()-'45d'::interval, now()-'10d'::interval),
    -- Split payment example (ORD-000010 paid partly by wallet)
    ('ORD-000010','wallet',        'paypal',  'PP_SPLIT_001', 'captured', NULL,           now()-'40d'::interval, now()-'40d'::interval)
) AS p(
    order_ref,
    payment_method,
    payment_provider,
    provider_ref,
    status,
    failure_reason,
    captured_at,
    updated_at
)
JOIN public.orders o
    ON o.order_ref = p.order_ref
ON CONFLICT DO NOTHING;


-- ═══════════════════════════════════════════════════════════════════════════
-- SECTION 5 — SIMULATE PIPELINE-REALISTIC UPDATE PATTERNS
-- These updates touch updated_at so the ETL watermark picks them up
-- ═══════════════════════════════════════════════════════════════════════════

-- Recent status transitions (last 24 hours — will be in today's ETL run)
UPDATE public.orders SET status = 'confirmed'  WHERE order_ref = 'ORD-000022';
UPDATE public.orders SET status = 'processing' WHERE order_ref = 'ORD-000017';
UPDATE public.orders SET status = 'shipped'    WHERE order_ref = 'ORD-000021';

-- Customer segment change (tests incremental customer extract)
UPDATE public.customers SET segment = 'enterprise' WHERE email = 'bob.smith@techwave.io';

-- Product price update (tests SCD handling in dbt snapshot)
UPDATE public.products SET unit_price = 1099.99 WHERE sku = 'SOFT-ANLYT-001';


-- ═══════════════════════════════════════════════════════════════════════════
-- SECTION 6 — VERIFICATION QUERIES
-- Run these after the inserts to confirm data integrity
-- ═══════════════════════════════════════════════════════════════════════════

-- 6.1  Row counts per table
SELECT 'customers'   AS tbl, COUNT(*) AS rows FROM public.customers
UNION ALL
SELECT 'products',              COUNT(*) FROM public.products
UNION ALL
SELECT 'orders',                COUNT(*) FROM public.orders
UNION ALL
SELECT 'order_items',           COUNT(*) FROM public.order_items
UNION ALL
SELECT 'payments',              COUNT(*) FROM public.payments
ORDER BY tbl;

-- 6.2  Orders by status
SELECT status, COUNT(*) AS order_count, ROUND(SUM(order_total)::numeric, 2) AS total_revenue
FROM public.orders
GROUP BY status
ORDER BY order_count DESC;

-- 6.3  Revenue by customer (validate joins work)
SELECT
    c.full_name,
    c.segment,
    COUNT(DISTINCT o.order_id)  AS orders,
    ROUND(SUM(o.order_total)::numeric, 2) AS lifetime_value
FROM public.customers c
JOIN public.orders o ON o.customer_id = c.customer_id
WHERE o.status NOT IN ('cancelled','refunded')
GROUP BY c.full_name, c.segment
ORDER BY lifetime_value DESC;

-- 6.4  Confirm updated_at triggers fire
SELECT table_name, MAX(updated_at) AS latest_update
FROM (
    SELECT 'customers' AS table_name, updated_at FROM public.customers
    UNION ALL SELECT 'products', updated_at FROM public.products
    UNION ALL SELECT 'orders',   updated_at FROM public.orders
    UNION ALL SELECT 'payments', updated_at FROM public.payments
) t
GROUP BY table_name
ORDER BY table_name;

-- 6.5  ETL watermark simulation — what the extractor would pull since yesterday
SELECT 'orders' AS tbl,
       COUNT(*) AS rows_since_yesterday,
       MIN(updated_at) AS min_wm,
       MAX(updated_at) AS max_wm
FROM public.orders
WHERE updated_at > now() - interval '1 day'
UNION ALL
SELECT 'customers',
       COUNT(*), MIN(updated_at), MAX(updated_at)
FROM public.customers
WHERE updated_at > now() - interval '1 day';

-- 6.6  FK integrity — no orphaned order_items or payments
SELECT 'orphaned_items'    AS check_name, COUNT(*) AS violations
FROM public.order_items i
LEFT JOIN public.orders o ON o.order_id = i.order_id
WHERE o.order_id IS NULL
UNION ALL
SELECT 'orphaned_payments', COUNT(*)
FROM public.payments p
LEFT JOIN public.orders o ON o.order_id = p.order_id
WHERE o.order_id IS NULL;