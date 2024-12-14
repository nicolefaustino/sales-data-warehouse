-- Start Product Dimension Procedure
CREATE OR REPLACE PROCEDURE create_product_dimension()
LANGUAGE plpgsql
AS
$$

DECLARE
	product_record RECORD;
	new_product_id VARCHAR(255);
	row_num INT := 1;
	product_cursor CURSOR FOR
		SELECT product_name, price_each, order_date
		FROM all_products
		ORDER BY order_date;

BEGIN

-- Drop Tables if exists
DROP TABLE IF EXISTS product_dimension CASCADE;
DROP TABLE IF EXISTS all_products CASCADE;
	

	-- All products table
	CREATE TABLE IF NOT EXISTS all_products(
		product_name TEXT,
		price_each DECIMAL(10, 2),
		order_date DATE
	);
	
	INSERT INTO all_products(product_name, price_each, order_date)
	SELECT c.product, c.price_each, c.order_date
	FROM cleansed AS c
	WHERE NOT EXISTS(
		SELECT 1
		FROM all_products a
		WHERE a.product_name = c.product
		  AND a.price_each = c.price_each
		  AND a.order_date = c.order_date
	);
	
	-- Product dimension table
	CREATE TABLE IF NOT EXISTS product_dimension(
		product_id TEXT,
		product_name TEXT,
		price_each DECIMAL(10, 2),
		last_update_date DATE,
		active_status CHAR(1),
		action_flag CHAR(1)
	);
	
	-- Cursor
	OPEN product_cursor;
	
		
	LOOP
		FETCH product_cursor INTO product_record;
		EXIT WHEN NOT FOUND;
		
		new_product_id := 'P' || LPAD(CAST(MD5(product_record.product_name) AS TEXT), 6, '0');
		
		-- Check if the product with the same name and price is already active
		IF NOT EXISTS (
			SELECT 1 
			FROM product_dimension pd
			WHERE pd.product_name = product_record.product_name
			  AND pd.price_each = product_record.price_each

		) THEN
			
			-- Insert the new product record
			INSERT INTO product_dimension (
				product_id,
				product_name,
				price_each,
				last_update_date,
				active_status,
				action_flag
			)
			VALUES (
				new_product_id,
				product_record.product_name,
				product_record.price_each,
				product_record.order_date,
				'Y',    -- Newer/higher price record is active
				'I'     -- Action flag 'I' for Insert
			);
			
-- Update higher price records to have action_flag = 'U'
			 
UPDATE product_dimension pd
SET 
    active_status = CASE
                       -- If this is the newer product with a different price
                       WHEN pd.product_name = product_record.product_name
                       AND pd.price_each != product_record.price_each
                       AND pd.last_update_date < product_record.order_date
                       THEN 'N' -- Set older product to inactive
						
                       -- If this is the new product
                       WHEN pd.product_name = product_record.product_name
                       AND pd.price_each = product_record.price_each
                       AND pd.last_update_date = product_record.order_date
                       THEN 'Y' -- Set the active product to active status
                       
                       ELSE pd.active_status -- No change to active_status
                   END,

    action_flag = CASE

                    	WHEN pd.product_name = product_record.product_name
                    	AND pd.price_each != product_record.price_each
						AND pd.last_update_date != '2019-01-01'
                    	THEN 'U' -- Action flag is 'U' for Update

					-- If the new product is the most recent one
                    	WHEN pd.product_name = product_record.product_name
                    	AND pd.price_each = product_record.price_each
                    	AND pd.last_update_date = product_record.order_date
                    	THEN 'U' -- Action flag is 'U' for Update
                    
                    ELSE pd.action_flag -- No change to action_flag
                  END
		WHERE pd.product_name = product_record.product_name
		  AND EXISTS (
			  -- Check if there are at least 2 records with the same product name
			  SELECT 1
			  FROM product_dimension pd2
			  WHERE pd2.product_name = pd.product_name
			  HAVING COUNT(*) > 1
		  );

		ELSE
			
			-- If the product exists with the same price, no action needed
			CONTINUE;
			
		END IF;
	END LOOP;
	
	-- Close the cursor
	CLOSE product_cursor;
	
		WITH product_sequence AS (
			SELECT product_name,
				   DENSE_RANK() OVER (ORDER BY product_name) AS seq_num  --dense rank to not skip any prod
			FROM product_dimension
			GROUP BY product_name
		)
		UPDATE product_dimension pd
		SET product_id = 'P' || LPAD(CAST(product_sequence.seq_num AS TEXT), 6, '0')
		FROM product_sequence
		WHERE pd.product_name = product_sequence.product_name;

RAISE NOTICE 'Product Dimension completed.';
COMMIT;
END;
$$;
