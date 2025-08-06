CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
  RAISE NOTICE '[%] load bronze started', to_char(clock_timestamp(), 'HH24:MI:SS');
TRUNCATE TABLE bronze.crm_cust_info;
COPY bronze.crm_cust_info
FROM '/datasets/source_crm/cust_info.csv'
WITH (FORMAT csv, HEADER true);

TRUNCATE TABLE bronze.crm_prd_info;
COPY bronze.crm_prd_info
FROM '/datasets/source_crm/prd_info.csv'
WITH (FORMAT csv, HEADER true);

TRUNCATE TABLE crm_sales_details;
COPY bronze.crm_sales_details
FROM '/datasets/source_crm/sales_details.csv'
WITH (FORMAT csv, HEADER true ); 

TRUNCATE TABLE bronze.erp_cust_az12;
COPY bronze.erp_cust_az12
FROM '/datasets/source_erp/CUST_AZ12.csv'
WITH (FORMAT csv, HEADER true);
TRUNCATE TABLE bronze.erp_loc_a101;
COPY bronze.erp_loc_a101
FROM '/datasets/source_erp/LOC_A101.csv'
WITH (FORMAT csv, HEADER true);
TRUNCATE TABLE bronze.erp_px_cat_g1v2;
COPY bronze.erp_px_cat_g1v2
FROM '/datasets/source_erp/PX_CAT_G1V2.csv'
WITH (FORMAT csv, HEADER true);
RAISE NOTICE '[%] load bronze finished', to_char(clock_timestamp(), 'HH24:MI:SS');
END;
$$;

GRANT EXECUTE ON PROCEDURE bronze.load_bronze TO PUBLIC;

GRANT USAGE ON SCHEMA bronze TO PUBLIC;

CALL bronze.load_bronze();