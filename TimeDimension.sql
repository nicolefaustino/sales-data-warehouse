-- Start Time Dimension Procedure
CREATE OR REPLACE PROCEDURE create_time_dimension()
LANGUAGE plpgsql
AS $$

DECLARE
	earliest_date DATE;
    latest_date DATE;
    loop_date DATE;
    
    -- Counters and tracking variables
    day_counter INTEGER := 0;
    week_counter INTEGER := 1;
    month_counter INTEGER := 1;
    quarter_counter INTEGER := 1;
    half_year_counter INTEGER := 1;
    year_counter INTEGER := 1;
    
    -- ID variables
    day_id VARCHAR;
    week_id VARCHAR;
    month_id VARCHAR;
    quarter_id VARCHAR;
    half_year_id VARCHAR;
    year_id VARCHAR;
    
    -- Tracking variables for hierarchy
    current_year INTEGER := 0;
    current_month INTEGER := 0;
    current_week INTEGER := 0;

BEGIN

-- Drop Tables if exists
DROP TABLE IF EXISTS time_dimension;
DROP TABLE IF EXISTS half_year_staging;
DROP TABLE IF EXISTS quarter_staging;
DROP TABLE IF EXISTS month_staging;
DROP TABLE IF EXISTS week_staging;
DROP TABLE IF EXISTS day_staging;
DROP TABLE IF EXISTS year_staging;
DROP TABLE IF EXISTS all_dates; 

    -- Determine date range 
    SELECT MIN(order_date), MAX(order_date) 
    INTO earliest_date, latest_date FROM cleansed;

    -- Temporary series of dates
    CREATE TEMP TABLE all_dates AS
    SELECT generate_series(earliest_date, latest_date, interval '1 day')::DATE AS order_date;

    -- Hierarchy tables
    CREATE TABLE day_staging (
		day_id VARCHAR PRIMARY KEY,
		day DATE NOT NULL,
		parent_id VARCHAR NOT NULL
		);

    CREATE TABLE week_staging (
		week_id VARCHAR PRIMARY KEY,
		week VARCHAR NOT NULL,
		parent_id VARCHAR NOT NULL
		);

    CREATE TABLE month_staging (
		month_id VARCHAR PRIMARY KEY,
		month VARCHAR NOT NULL,
		parent_id VARCHAR NOT NULL
		);

    CREATE TABLE quarter_staging (
		quarter_id VARCHAR PRIMARY KEY,
		quarter VARCHAR NOT NULL,
		parent_id VARCHAR NOT NULL
		);

    CREATE TABLE half_year_staging (
		half_year_id VARCHAR PRIMARY KEY,
		half_year VARCHAR NOT NULL,
		parent_id VARCHAR NOT NULL
		);

    CREATE TABLE year_staging (
        year_id VARCHAR PRIMARY KEY,
        year VARCHAR NOT NULL,
        parent_id VARCHAR
    	);

    CREATE TABLE time_dimension (time_id VARCHAR PRIMARY KEY,
        time_desc DATE,
        week_id VARCHAR,
        week_desc VARCHAR,
        month_id VARCHAR,
        month_desc VARCHAR,
        quarter_id VARCHAR,
        quarter_desc VARCHAR,
        half_year_id VARCHAR,
        half_year_desc VARCHAR,
        year_id VARCHAR,
        year_desc VARCHAR
    	);

FOR
	loop_date IN (SELECT order_date FROM all_dates ORDER BY order_date) LOOP
IF current_year != EXTRACT(YEAR FROM loop_date) THEN
	current_year := EXTRACT(YEAR FROM loop_date);
	year_id := 'Y' || TO_CHAR(loop_date, 'YYYY');
            
INSERT INTO year_staging (
	year_id,
	year,
	parent_id
	)
VALUES (
	year_id,
	current_year::TEXT, NULL
	);
            
year_counter := year_counter + 1;
END IF;

-- Half-year processing
IF EXTRACT(MONTH FROM loop_date) IN (1, 7) AND EXTRACT(DAY FROM loop_date) = 1 THEN
	half_year_id := 'H' || LPAD(half_year_counter::TEXT, 3, '0');
            
INSERT INTO half_year_staging (
	half_year_id,
	half_year,
	parent_id
	)
VALUES (
	half_year_id, 
	'H' || TO_CHAR(loop_date, 'YYYY-HH'), year_id
	);
            
half_year_counter := half_year_counter + 1;
END IF;

-- Quarter processing
IF EXTRACT(MONTH FROM loop_date) IN (1, 4, 7, 10) AND EXTRACT(DAY FROM loop_date) = 1 THEN
	quarter_id := 'Q' || LPAD(quarter_counter::TEXT, 3, '0');
            
INSERT INTO quarter_staging (
	quarter_id,
	quarter,
	parent_id
	)
VALUES (
	quarter_id,
	'Q' || TO_CHAR(loop_date, 'QYY'), half_year_id
	);
            
quarter_counter := quarter_counter + 1;
END IF;

-- Month processing
IF current_month != EXTRACT(MONTH FROM loop_date) THEN
	current_month := EXTRACT(MONTH FROM loop_date);
	month_id := 'M' || LPAD(month_counter::TEXT, 3, '0');
            
INSERT INTO month_staging (
	month_id, 
	month, 
	parent_id
	)
VALUES (
	month_id,
	'M' || TO_CHAR(loop_date, 'MMYY'), quarter_id
	);
            
month_counter := month_counter + 1;
END IF;

-- Week processing
IF current_week != EXTRACT(WEEK FROM loop_date) THEN
	current_week := EXTRACT(WEEK FROM loop_date);
	week_id := 'W' || LPAD(week_counter::TEXT, 3, '0');
            
INSERT INTO week_staging (
	week_id, 
	week, 
	parent_id
	)
VALUES (
	week_id,
	'W' || TO_CHAR(loop_date, 'WWYY'), month_id
	);
            
week_counter := week_counter + 1;
END IF;

-- Day processing
day_id := 'D' || TO_CHAR(loop_date, 'YYYYDDMM');
        
INSERT INTO day_staging (
	day_id, 
	day, 
	parent_id
	)
VALUES (
	day_id,
	loop_date,
	week_id
	);
	END LOOP;

-- Populate time dimension
INSERT INTO time_dimension (
	time_id,
	time_desc,
	week_id,
	week_desc,
	month_id,
	month_desc,
	quarter_id,
	quarter_desc,
	half_year_id,
	half_year_desc,
	year_id,
	year_desc
    )
SELECT
	d.day_id AS time_id,
	d.day AS time_desc,
	w.week_id,
	w.week AS week_desc,
	m.month_id,
	m.month AS month_desc,
	q.quarter_id,
	q.quarter AS quarter_desc,
	h.half_year_id,
	h.half_year AS half_year_desc,
	y.year_id,
	y.year AS year_desc
FROM
	day_staging d
JOIN
	week_staging w
ON
	d.parent_id = w.week_id
JOIN
	month_staging m 
ON
	w.parent_id = m.month_id
JOIN 
	quarter_staging q 
ON 
	m.parent_id = q.quarter_id
JOIN
	half_year_staging h
ON
	q.parent_id = h.half_year_id
JOIN 
	year_staging y
ON
	h.parent_id = y.year_id;

RAISE NOTICE 'Time Dimension completed.';
END;
$$;