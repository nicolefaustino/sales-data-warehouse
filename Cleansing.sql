-- Start Cleansing and Normalization Procedure
CREATE OR REPLACE PROCEDURE cleansing_and_normalization()
LANGUAGE plpgsql
AS $$

DECLARE
	row RECORD;
    max_order_id INT;
    street TEXT;
    city TEXT;
    state TEXT;
    postal TEXT;

BEGIN

SELECT COALESCE(MAX(order_id), 0) INTO max_order_id FROM cleansed;

-- Process rows from the landing table into to_process table
FOR row IN SELECT * FROM landing
LOOP
INSERT INTO
	to_process (order_id, product, quantity_ordered, price_each, order_date, purchase_address)
	VALUES (
		row.order_id,
		row.product,
		row.quantity_ordered,
		row.price_each,
		row.order_date,
		row.purchase_address
        );
		
END LOOP;

-- Process rows in the to_process table
FOR row IN SELECT * FROM to_process
LOOP
IF 
	row.product IS NULL OR row.quantity_ordered IS NULL OR row.price_each IS NULL
OR
	row.order_date IS NULL OR row.purchase_address IS NULL THEN

INSERT INTO invalid (order_id, product, quantity_ordered, price_each, order_date, purchase_address)
	VALUES (
		row.order_id,
		row.product,
		row.quantity_ordered,
		row.price_each,
		row.order_date,
		row.purchase_address
		);
	DELETE FROM
		to_process WHERE ctid = row.ctid;
	CONTINUE;
	END IF;

-- Assign missing order_id
IF
	row.order_id IS NULL THEN
	max_order_id := max_order_id + 1;
	row.order_id := max_order_id;
END IF;

-- Normalize purchase_address
BEGIN
	street := SPLIT_PART(TRIM(row.purchase_address), ',', 1);
	city := TRIM(SPLIT_PART(row.purchase_address, ',', 2));
	state := SPLIT_PART(TRIM(SPLIT_PART(row.purchase_address, ',', 3)), ' ', 1);
	postal := SPLIT_PART(TRIM(SPLIT_PART(row.purchase_address, ',', 3)), ' ', 2);

-- Move to cleansed table
INSERT INTO
	cleansed (order_id, product, quantity_ordered, price_each, order_date, street, city, state, postal)
	VALUES (
		row.order_id::INT,
		TRIM(row.product),
		row.quantity_ordered::INT,
		row.price_each::DECIMAL(10, 2),
		CAST(TO_TIMESTAMP(row.order_date, 'MM-DD-YY HH24:MI') AS DATE),
		street,
		city,
		state,
		postal
		);

DELETE FROM
	to_process WHERE ctid = row.ctid;
	EXCEPTION
	WHEN OTHERS THEN

INSERT INTO invalid (
	order_id,
	product,
	quantity_ordered,
	price_each,
	order_date,
	purchase_address)
VALUES (
	row.order_id,
	row.product,
	row.quantity_ordered,
	row.price_each,
	row.order_date,
	row.purchase_address
	);
	DELETE FROM to_process WHERE ctid = row.ctid;
	END;
    END LOOP;

-- Order cleansed table by order_date
CREATE TEMP TABLE cleansed_temp AS
	SELECT *
	FROM 
		cleansed
    ORDER BY
		order_date ASC;

TRUNCATE TABLE cleansed;
	INSERT INTO 
		cleansed
	SELECT DISTINCT *
	FROM
		cleansed_temp;
	DROP TABLE
		cleansed_temp;

RAISE NOTICE 'Cleansing and Normalization completed.';
END;
$$;
