-- createdb database_wb

-- CREATE ROLE user_wb with encrypted password 'pass';
-- GRANT ALL PRIVILEGES ON DATABASE database_wb to wb_user;

--CREATE TABLE "orders" (
--    id SERIAL PRIMARY KEY,
--    uid VARCHAR(20) UNIQUE NOT NULL,
--    data TEXT NOT NULL
--);

-- CREATE TABLE payment (
--     id SERIAL PRIMARY KEY,
--     transaction VARCHAR(50) UNIQUE,
--     request_id VARCHAR(20),
--     currency CHAR(3) NOT NULL,
--     provider VARCHAR(20) NOT NULL,
--     amount INT NOT NULL,
--     payment_dt INT NOT NULL,
--     bank VARCHAR(50) NOT NULL,
--     delivery_cost INT,
--     goods_total INT,
--     custom_fee INT
-- );

-- CREATE TABLE item (
--     id SERIAL PRIMARY KEY,
--     chrt_id INT,
--     track_number VARCHAR(20),
--     price INT NOT NULL,
--     rid VARCHAR(50),
--     name VARCHAR(50) NOT NULL,
--     sale INT,
--     size VARCHAR(10),
--     total_price INT,
--     nm_id INT,
--     brand VARCHAR(50),
--     status INT
-- );

-- CREATE TABLE delivery (
--     id SERIAL PRIMARY KEY,
--     name VARCHAR(50) NOT NULL,
--     phone VARCHAR(12) NOT NULL,
--     zip VARCHAR(10) NOT NULL,
--     city VARCHAR(50) NOT NULL,
--     address VARCHAR(50) NOT NULL,
--     region VARCHAR(50) NOT NULL,
--     email VARCHAR(50) NOT NULL
-- );

-- CREATE TABLE "order" (
--     id SERIAL PRIMARY KEY,
--     order_uid VARCHAR(50) UNIQUE NOT NULL,
--     track_number VARCHAR(20) NOT NULL,
--     entry VARCHAR(10),
--     delivery_id INT NOT NULL,
--     payment_id INT NOT NULL,
--     locale VARCHAR(10) NOT NULL,
--     internal_signature VARCHAR(20),
--     customer_id VARCHAR(50) NOT NULL,
--     delivery_service VARCHAR(20) NOT NULL,
--     shardkey VARCHAR(10),
--     sm_id INT,
--     date_created TIMESTAMPTZ NOT NULL,
--     oof_shard VARCHAR(10),
--     CONSTRAINT fk_delivery
--         FOREIGN KEY(delivery_id)
--             REFERENCES delivery(id),
--     CONSTRAINT fk_payment
--         FOREIGN KEY(payment_id)
--             REFERENCES payment(id)
-- );

-- CREATE TABLE order_item (
--     order_id INT,
--     item_id INT,
--     PRIMARY KEY(order_id, item_id),
--     CONSTRAINT fk_order
--         FOREIGN KEY(order_id)
--             REFERENCES "order"(id),
--     CONSTRAINT fk_item
--         FOREIGN KEY(item_id)
--             REFERENCES item(id)
-- );

-- CREATE VIEW everything AS
-- SELECT
--     order_uid, "order".track_number, entry,
--     delivery.name as del_name, phone, zip, city, address, region, email, -- delivery
--     transaction, request_id, currency, provider, amount, -- payment
--     payment_dt, bank, delivery_cost, goods_total, custom_fee, -- payment
--     chrt_id, item.track_number as item_tn, price, rid, -- item                  
--     item.name as item_name, sale, size, total_price, nm_id, brand, status -- item
--     locale, internal_signature, customer_id, delivery_service, shardkey,
--     sm_id, date_created, oof_shard 
-- FROM order_item
-- JOIN "order"
--     ON "order".id = order_item.order_id
-- JOIN delivery
--     ON "order".delivery_id = delivery.id
-- JOIN payment
--     ON "order".payment_id = payment.id
-- JOIN item
--     ON order_item.item_id = item.id;

-- CREATE TABLE timetest (
--     date TIMESTAMPTZ NOT NULL
-- );

-- INSERT INTO timetest (date) VALUES ('2021-11-26T06:22:19Z');

-- SELECT to_char(date, 'YYYY-MM-DD"T"HH:MI:SS TZ') FROM timetest;
-- SELECT to_char(date AT TIME ZONE 'z', 'YYYY-MM-DD"T"HH:MI:SSZ') FROM timetest;

-- INSERT INTO item (chrt_id, track_number, price, rid, name, sale, size, 
--             total_price, nm_id, brand, status)
--     VALUES (9934930, 'WBLILTRACK', 453, 'ab6423nbdf94test', 'Redhat',
--             30, 0, 317, 23454, 'hm', 202);

-- INSERT INTO item (chrt_id, track_number, price, rid, name, sale, size, 
--             total_price, nm_id, brand, status)
--     VALUES (123, 'WBLSDFTRACK', 123, 'shadfas932', 'blackhat',
--             30, 0, 317, 23454, 'hm', 202);

-- INSERT INTO payment (transaction, request_id, currency, provider, amount,
--             payment_dt, bank, delivery_cost, goods_total, custom_fee)
--     VALUES ('b56feb67bfc', '', 'USD', 'wbpay', '1817', 163790233, 'alpha',
--             1500, 317, 0);

-- INSERT INTO delivery (name, phone, zip, city, address, region, email)
--     VALUES ('Test Testov', '+9720000000', '2639809', 'Kiryat Mozkin',
--             'Ploshad Mira 15', 'Kraiot', 'test@gmail.com');

-- INSERT INTO "order" (order_uid, track_number, entry, delivery_id, payment_id,
--                     locale, internal_signature, customer_id, delivery_service,
--                     shardkey, sm_id, date_created, oof_shard)
--     VALUES ('b563feb7b2b84b6test', 'WBILMTESTTRACK', 'WBIL', 1, 1, 'en', '',
--             'test', 'meest', '9', 99, '2021-11-26T06:22:19Z', '1');

-- INSERT INTO order_item VALUES (1, 1);
