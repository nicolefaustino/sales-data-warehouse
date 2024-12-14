-- Start Import Procedure
CREATE OR REPLACE PROCEDURE import(filepath TEXT)
LANGUAGE plpgsql
AS $$
BEGIN

-- Landing table
IF NOT EXISTS (
	SELECT 1
	FROM
		information_schema.tables
	WHERE
		table_name = 'landing'
    ) THEN
	EXECUTE 'CREATE TABLE landing (
            order_id TEXT,
            product TEXT,
            quantity_ordered TEXT,
            price_each TEXT,
            order_date TEXT,
            purchase_address TEXT
        )';
		END IF;

-- Invalid table
IF NOT EXISTS (
	SELECT 1
	FROM
		information_schema.tables
	WHERE table_name = 'invalid'
    ) THEN
	EXECUTE 'CREATE TABLE invalid (
            order_id TEXT,
            product TEXT,
            quantity_ordered TEXT,
            price_each TEXT,
            order_date TEXT,
            purchase_address TEXT
        )';
		END IF;

-- To_process table
IF NOT EXISTS (
	SELECT 1
	FROM
		information_schema.tables
	WHERE
		table_name = 'to_process'
    ) THEN
        EXECUTE 'CREATE TABLE to_process (
            order_id TEXT,
            product TEXT,
            quantity_ordered TEXT,
            price_each TEXT,
            order_date TEXT,
            purchase_address TEXT
        )';
    END IF;

-- Cleansed table
IF NOT EXISTS (
	SELECT 1
	FROM
		information_schema.tables
	WHERE
		table_name = 'cleansed'
    ) THEN
        EXECUTE 'CREATE TABLE cleansed (
            order_id INT,
            product TEXT,
            quantity_ordered INT,
            price_each DECIMAL(10, 2),
            order_date TIMESTAMP,
            street TEXT,
            city TEXT,
            state TEXT,
            postal TEXT
        )';
		END IF;

-- Import data
BEGIN
EXECUTE format(
	'COPY landing(order_id, product, quantity_ordered, price_each, order_date, purchase_address)
	FROM %L
	WITH (FORMAT CSV, HEADER, DELIMITER '','');',
	filepath
	);
	EXCEPTION WHEN OTHERS THEN
		RAISE NOTICE 'Error importing file: %', SQLERRM;
		END;

END;
$$;