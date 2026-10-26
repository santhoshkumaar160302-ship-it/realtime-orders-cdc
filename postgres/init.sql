-- Source OLTP schema for a small e-commerce store.
-- This is the "production" database. The pipeline never queries it directly
-- for analytics; every change is captured from the write-ahead log (WAL).

CREATE TABLE customers (
    customer_id  BIGSERIAL PRIMARY KEY,
    full_name    TEXT        NOT NULL,
    email        TEXT        NOT NULL,
    state        TEXT        NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE products (
    product_id   BIGSERIAL PRIMARY KEY,
    name         TEXT          NOT NULL,
    category     TEXT          NOT NULL,
    unit_price   NUMERIC(10,2) NOT NULL CHECK (unit_price >= 0),
    updated_at   TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE TABLE orders (
    order_id      BIGSERIAL PRIMARY KEY,
    customer_id   BIGINT        NOT NULL REFERENCES customers (customer_id),
    status        TEXT          NOT NULL
                  CHECK (status IN ('placed', 'paid', 'shipped', 'delivered', 'cancelled')),
    total_amount  NUMERIC(12,2) NOT NULL CHECK (total_amount >= 0),
    created_at    TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE TABLE order_items (
    order_item_id BIGSERIAL PRIMARY KEY,
    order_id      BIGINT        NOT NULL REFERENCES orders (order_id) ON DELETE CASCADE,
    product_id    BIGINT        NOT NULL REFERENCES products (product_id),
    quantity      INT           NOT NULL CHECK (quantity > 0),
    unit_price    NUMERIC(10,2) NOT NULL CHECK (unit_price >= 0)
);

CREATE INDEX idx_orders_status   ON orders (status);
CREATE INDEX idx_orders_customer ON orders (customer_id);
CREATE INDEX idx_items_order     ON order_items (order_id);

-- REPLICA IDENTITY FULL makes Postgres write the complete old row to the WAL
-- on UPDATE/DELETE, so Debezium can emit a full "before" image (useful for
-- auditing and SCD history), not only the primary key.
ALTER TABLE customers   REPLICA IDENTITY FULL;
ALTER TABLE products    REPLICA IDENTITY FULL;
ALTER TABLE orders      REPLICA IDENTITY FULL;
ALTER TABLE order_items REPLICA IDENTITY FULL;
