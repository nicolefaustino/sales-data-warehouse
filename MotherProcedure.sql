-- Start of ETL Process
CREATE OR REPLACE PROCEDURE etl(filepath TEXT)
LANGUAGE plpgsql
AS $$
BEGIN
    -- Importing
    CALL import(filepath);
    
    -- Cleansing
    CALL cleansing_and_normalization();

    -- Location Dimension
    CALL create_location_dimension();

    -- Time Dimension
    CALL create_time_dimension();

    -- Product Dimension
    CALL create_product_dimension();

    -- Fact Table
    CALL create_fact_table();

END;
$$;
