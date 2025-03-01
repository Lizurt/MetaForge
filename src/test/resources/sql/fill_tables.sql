DO '
DECLARE
    user_count INT := 1000;
    product_count INT := 1000;
    order_count INT := 1000;
    log_count INT := 1000;
    payment_count INT := 1000;
BEGIN
TRUNCATE TABLE users, products, orders, logs, payments RESTART IDENTITY CASCADE;

-- Insert users
FOR i IN 1..user_count LOOP
    INSERT INTO users (id, name, birth_date, email)
    VALUES (gen_random_uuid()::uuid, ''User_'' || i, ''1990-01-01''::DATE + (i % 365), ''user_'' || i || ''@example.com'');
    END LOOP;

-- Insert products
FOR i IN 1..product_count LOOP
    INSERT INTO products (name, price)
    VALUES (''Product_'' || i, random() * 1000 + 1);
    END LOOP;

-- Insert orders
FOR i IN 1..order_count LOOP
    INSERT INTO orders (id, user_id, product_id, quantity, order_date)
    VALUES (''ORD_'' || i, (SELECT id FROM users ORDER BY random() LIMIT 1),
            (SELECT id FROM products ORDER BY random() LIMIT 1),
        (random() * 10 + 1)::INT, CURRENT_TIMESTAMP - (i || '' days'')::INTERVAL);
    END LOOP;

-- Insert logs
FOR i IN 1..log_count LOOP
    INSERT INTO logs (event_time, event_type, message)
    VALUES (CURRENT_TIMESTAMP - (i || '' minutes'')::INTERVAL,
        CASE WHEN random() < 0.7 THEN ''INFO'' WHEN random() < 0.9 THEN ''WARN'' ELSE ''ERROR'' END,
        ''Log message '' || i);
    END LOOP;

-- Insert payments
FOR i IN 1..payment_count LOOP
    INSERT INTO payments (order_id, amount, paid_at)
    VALUES ((SELECT id FROM orders ORDER BY random() LIMIT 1), random() * 1000 + 1, CURRENT_TIMESTAMP - (i || '' hours'')::INTERVAL);
    END LOOP;

INSERT INTO users_broken SELECT id::TEXT, name, birth_date::TEXT, email FROM users;
INSERT INTO products_broken SELECT id::TEXT, name, price::TEXT FROM products;
INSERT INTO orders_broken SELECT id::TEXT, user_id::TEXT, product_id::TEXT, quantity::TEXT, order_date::TEXT FROM orders;
INSERT INTO logs_broken SELECT id::TEXT, event_time::TEXT, event_type, message FROM logs;
INSERT INTO payments_broken SELECT id::TEXT, order_id::TEXT, amount::TEXT, paid_at::TEXT FROM payments;
END ' language plpgsql;
