WHENEVER SQLERROR EXIT SQL.SQLCODE

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE orders PURGE';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE != -942 THEN RAISE; END IF;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE products PURGE';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE != -942 THEN RAISE; END IF;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE customers PURGE';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE != -942 THEN RAISE; END IF;
END;
/

CREATE TABLE customers (
    customer_id NUMBER PRIMARY KEY,
    name        VARCHAR2(100) NOT NULL,
    email       VARCHAR2(200) NOT NULL,
    city        VARCHAR2(100) NOT NULL
);

CREATE TABLE products (
    product_id NUMBER PRIMARY KEY,
    name       VARCHAR2(100) NOT NULL,
    price      NUMBER(10, 2) NOT NULL
);

CREATE TABLE orders (
    order_id   NUMBER PRIMARY KEY,
    customer_id NUMBER NOT NULL REFERENCES customers(customer_id),
    product_id  NUMBER NOT NULL REFERENCES products(product_id),
    quantity    NUMBER NOT NULL
);

INSERT INTO customers (customer_id, name, email, city)
VALUES (1, 'Ada Lovelace', 'ada@example.test', 'London');
INSERT INTO customers (customer_id, name, email, city)
VALUES (2, 'Grace Hopper', 'grace@example.test', 'New York');
INSERT INTO customers (customer_id, name, email, city)
VALUES (3, 'Linus Torvalds', 'linus@example.test', 'Helsinki');

INSERT INTO products (product_id, name, price)
VALUES (101, 'Arrow Notebook', 12.50);
INSERT INTO products (product_id, name, price)
VALUES (102, 'DuckDB Mug', 18.00);
INSERT INTO products (product_id, name, price)
VALUES (103, 'Oracle Sticker Pack', 6.75);

INSERT INTO orders (order_id, customer_id, product_id, quantity)
VALUES (1001, 1, 101, 2);
INSERT INTO orders (order_id, customer_id, product_id, quantity)
VALUES (1002, 2, 102, 1);
INSERT INTO orders (order_id, customer_id, product_id, quantity)
VALUES (1003, 3, 103, 4);

COMMIT;

SELECT 'customers' AS table_name, COUNT(*) AS row_count FROM customers
UNION ALL
SELECT 'products', COUNT(*) FROM products
UNION ALL
SELECT 'orders', COUNT(*) FROM orders;

EXIT;
