-- Start Location Dimension Procedure
CREATE OR REPLACE PROCEDURE create_location_dimension()
LANGUAGE plpgsql
AS $$

DECLARE

-- Cursor 
    cur_cleansed CURSOR FOR SELECT street, city, state, postal FROM cleansed;

    current_street TEXT;
    current_city TEXT;
    current_state TEXT;
    current_postal TEXT;

    v_state_id TEXT;
    v_city_id TEXT;
    v_street_id TEXT;

BEGIN

-- Drop Tables if exists
DROP TABLE IF EXISTS street_staging;
DROP TABLE IF EXISTS city_staging;
DROP TABLE IF EXISTS state_staging;
DROP TABLE IF EXISTS location_dimension;
	
-- Create Staging Tables
IF NOT EXISTS
	(SELECT 1 FROM information_schema.tables WHERE table_name = 'state_staging') THEN
	
CREATE TABLE state_staging (
	state_id TEXT,
	state_name TEXT,
	postal TEXT
	);
    END IF;

IF NOT EXISTS
	(SELECT 1 FROM information_schema.tables WHERE table_name = 'city_staging') THEN

CREATE TABLE city_staging (
	city_id TEXT,
	city_name TEXT,
	state_id TEXT
	);
    END IF;

IF NOT EXISTS
	(SELECT 1 FROM information_schema.tables WHERE table_name = 'street_staging') THEN

CREATE TABLE street_staging (
	street_id TEXT,
	street_name TEXT,
	city_id TEXT
	);
	END IF;

IF NOT EXISTS
	(SELECT 1 FROM information_schema.tables WHERE table_name = 'location_dimension') THEN

CREATE TABLE location_dimension (
	location_id TEXT,
	street_id TEXT,
	street_name TEXT,
	city_id TEXT,
	city_name TEXT,
	state_id TEXT,
	state_name TEXT,
	postal TEXT,
	full_address TEXT
	);
    END IF;

-- Open Cursor
OPEN cur_cleansed;

LOOP

FETCH cur_cleansed INTO current_street, current_city, current_state, current_postal;

EXIT WHEN NOT FOUND;

-- Process State
SELECT
	state_id
INTO
	v_state_id
FROM
	state_staging
WHERE state_name = current_state AND postal = current_postal;

-- If state does not exist, insert it and generate a new state_id
IF v_state_id IS NULL THEN
	v_state_id := 'SA' || LPAD(CAST((SELECT COUNT(*) + 1 FROM state_staging) AS TEXT), 3, '0');
INSERT INTO
	state_staging (state_id, state_name, postal)
	VALUES (v_state_id, current_state, current_postal);
	END IF;

-- Process City
SELECT
	city_id
INTO
	v_city_id
FROM
	city_staging
WHERE city_name = current_city AND state_id = v_state_id;

-- If city does not exist, insert it and generate a new city_id
IF v_city_id IS NULL THEN
	v_city_id := 'CT' || LPAD(CAST((SELECT COUNT(*) + 1 FROM city_staging) AS TEXT), 3, '0');
INSERT INTO
	city_staging (city_id, city_name, state_id)
	VALUES (v_city_id, current_city, v_state_id);
	END IF;

-- Process Street
SELECT
	street_id
INTO
	v_street_id
FROM
	street_staging
WHERE street_name = current_street AND city_id = v_city_id;

-- If street does not exist, insert it and generate a new street_id
IF v_street_id IS NULL THEN
	v_street_id := 'ST' || LPAD(CAST((SELECT COUNT(*) + 1 FROM street_staging) AS TEXT), 3, '0');
	INSERT INTO street_staging (street_id, street_name, city_id)
	VALUES (v_street_id, current_street, v_city_id);
	END IF;
    END LOOP;

CLOSE cur_cleansed;

ALTER TABLE
	state_staging
RENAME COLUMN postal TO parent_loc_id;

ALTER TABLE
	city_staging 
RENAME COLUMN state_id TO parent_loc_id;

ALTER TABLE
	street_staging 
RENAME COLUMN city_id TO parent_loc_id;

-- Final Join
INSERT INTO location_dimension (
	location_id,
	street_id,
	street_name,
	city_id,
	city_name,
	state_id,
	state_name,
	postal,
	full_address
	)
SELECT
	'L' || LPAD(CAST(ROW_NUMBER() OVER (ORDER BY street.street_id, city.city_id, state.state_id) AS TEXT), 6, '0') AS location_id,  -- Generate location_id
	street.street_id,
	street.street_name,
	city.city_id,
	city.city_name,
	state.state_id,
	state.state_name,
	state.parent_loc_id AS postal,
	street.street_name || ', ' || city.city_name || ', ' || state.state_name || ' ' || state.parent_loc_id AS full_address
FROM
	street_staging street
INNER JOIN
	city_staging city
ON
	street.parent_loc_id = city.city_id
INNER JOIN
	state_staging state
ON
	city.parent_loc_id = state.state_id;

RAISE NOTICE 'Location Dimension completed.';
END;
$$;
