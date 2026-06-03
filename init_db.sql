-- init_db.sql
-- Example 3NF schema with proper PK / FK constraints.
-- GraFlo's heuristics need these to auto-classify tables as
-- vertex tables (entities) vs edge tables (relationships).
--
-- Replace this with your own schema — keep the PK/FK conventions
-- and inference will work the same way.

-- ── Vertex tables (entities) ─────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS users (
    id         SERIAL       PRIMARY KEY,
    name       VARCHAR(255) NOT NULL,
    email      VARCHAR(255) NOT NULL UNIQUE,
    created_at TIMESTAMP    NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS products (
    id          SERIAL         PRIMARY KEY,
    name        VARCHAR(255)   NOT NULL,
    price       DECIMAL(10, 2) NOT NULL,
    description TEXT,
    created_at  TIMESTAMP      NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS categories (
    id   SERIAL       PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE
);

-- ── Edge tables (relationships — 2+ FK columns) ───────────────────────────────

-- users → products  (purchase transaction)
CREATE TABLE IF NOT EXISTS purchases (
    id            SERIAL         PRIMARY KEY,
    user_id       INTEGER        NOT NULL REFERENCES users(id)    ON DELETE CASCADE,
    product_id    INTEGER        NOT NULL REFERENCES products(id) ON DELETE CASCADE,
    quantity      INTEGER        NOT NULL DEFAULT 1,
    total_amount  DECIMAL(10, 2) NOT NULL,
    purchase_date TIMESTAMP      NOT NULL DEFAULT NOW()
);

-- users → users  (social follow, self-referential)
CREATE TABLE IF NOT EXISTS follows (
    id          SERIAL    PRIMARY KEY,
    follower_id INTEGER   NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    followed_id INTEGER   NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    created_at  TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (follower_id, followed_id)
);

-- products → categories  (many-to-many junction)
CREATE TABLE IF NOT EXISTS product_categories (
    id          SERIAL    PRIMARY KEY,
    product_id  INTEGER   NOT NULL REFERENCES products(id)   ON DELETE CASCADE,
    category_id INTEGER   NOT NULL REFERENCES categories(id) ON DELETE CASCADE,
    UNIQUE (product_id, category_id)
);

-- ── Seed data ─────────────────────────────────────────────────────────────────

INSERT INTO users  (name, email)
VALUES
    ('Alice Martin',  'alice@example.com'),
    ('Bob Dupont',    'bob@example.com'),
    ('Claire Moreau', 'claire@example.com')
ON CONFLICT DO NOTHING;

INSERT INTO categories (name)
VALUES ('Electronics'), ('Books'), ('Clothing')
ON CONFLICT DO NOTHING;

INSERT INTO products (name, price, description)
VALUES
    ('Laptop Pro 15',   1299.99, 'High-performance laptop'),
    ('Wireless Earbuds',  79.99, 'Noise-cancelling earbuds'),
    ('Python Cookbook',   39.99, 'Advanced Python recipes')
ON CONFLICT DO NOTHING;

INSERT INTO purchases (user_id, product_id, quantity, total_amount)
VALUES
    (1, 1, 1, 1299.99),
    (1, 2, 2,  159.98),
    (2, 3, 1,   39.99),
    (3, 1, 1, 1299.99)
ON CONFLICT DO NOTHING;

INSERT INTO follows (follower_id, followed_id)
VALUES (1, 2), (2, 3), (3, 1)
ON CONFLICT DO NOTHING;

INSERT INTO product_categories (product_id, category_id)
VALUES (1, 1), (2, 1), (3, 2)
ON CONFLICT DO NOTHING;
