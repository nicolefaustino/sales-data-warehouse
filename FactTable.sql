-- Start of Fact Table Procedure
CREATE OR REPLACE PROCEDURE create_fact_table()
LANGUAGE plpgsql
AS $$

BEGIN

-- Drop tables if they already exist
DROP TABLE IF EXISTS fact_staging1;
DROP TABLE IF EXISTS fact_staging2;
DROP TABLE IF EXISTS fact_staging3;
DROP TABLE IF EXISTS fact_table;

-- Fact Staging 1
CREATE TABLE fact_staging1 AS
SELECT
	c.order_id, pd1.product_id, c.product,
	(SELECT 
	 	pd2.price_each
	 FROM
	 	product_dimension pd2
	 WHERE
	 	pd2.product_name = c.product
	 AND pd2.last_update_date <= c.order_date
	 ORDER BY pd2.last_update_date DESC
	 LIMIT 1) 
	 AS price_each,
	 c.quantity_ordered, c.order_date, c.street, c.city, c.state, c.postal
    FROM 
        cleansed c
    INNER JOIN 
        product_dimension pd1 
    ON
        pd1.product_name = c.product;

-- Fact Staging 2
CREATE TABLE fact_staging2 AS
SELECT
	f1.order_id,
	f1.product_id,
	f1.product,
	f1.price_each,
	f1.quantity_ordered,
	f1.order_date::DATE,
	l.location_id, f1.street, f1.city, f1.state, f1.postal
FROM
	fact_staging1 f1
INNER JOIN
	location_dimension l
ON  f1.street = l.street_name
AND f1.city = l.city_name
AND f1.state = l.state_name
AND f1.postal = l.postal;

-- Fact Staging 3
CREATE TABLE fact_staging3 AS
SELECT
	f2.order_id,
	f2.product_id,
	f2.product,
	f2.price_each,
	f2.quantity_ordered,
	t.time_id,
	f2.order_date,
	f2.location_id,
	f2.street,
	f2.city,
	f2.state,
	f2.postal
FROM
	fact_staging2 f2
INNER JOIN
	time_dimension t
ON
	f2.order_date = t.time_desc::DATE;

-- Final Fact Table
CREATE TABLE fact_table AS
SELECT
	t.time_id,
	pd.product_id,
	pd.product_name AS product,
	pd.price_each AS price_each,
	COALESCE(f3.quantity_ordered, 0) AS quantity_ordered,
	t.time_desc AS order_date, 
	t.week_id, 
	t.month_id, 
	t.quarter_id, 
	t.half_year_id, 
	t.year_id,
	l.location_id,
	l.street_name,
	l.street_id, 
	l.city_name,
	l.city_id, 
	l.state_name,
	l.state_id, 
	l.postal
FROM
	time_dimension t
CROSS JOIN
	product_dimension pd
CROSS JOIN
	location_dimension l
LEFT JOIN
	fact_staging3 f3
ON
	f3.product_id = pd.product_id
	AND f3.time_id = t.time_id
	AND f3.location_id = l.location_id;


RAISE NOTICE 'Fact Table completed.';
END;
$$;