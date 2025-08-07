CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  v_step TEXT;
  v_ok int := 0;
  v_err int := 0;
  v_msg TEXT;
  v_start_batch timestamp := clock_timestamp();
  v_end_batch timestamp;
BEGIN
  RAISE NOTICE '[%] load bronze started', to_char(clock_timestamp(), 'HH24:MI:SS');
  
  BEGIN
    DECLARE
      v_start_date timestamp  := clock_timestamp();
    BEGIN
    v_step := 'load bronze.crm_cust_info';
    RAISE NOTICE 'Step [%] started at %', v_step, to_char(v_start_date, 'HH24:MI:SS');
    TRUNCATE TABLE bronze.crm_cust_info;
    COPY bronze.crm_cust_info
      FROM '/datasets/source_crm/cust_info.csv'
      WITH (FORMAT csv, HEADER true);
    v_ok := v_ok + 1;
    RAISE NOTICE 'Step [%] completed successfully (%.0f s)',
             v_step,
             extract(epoch FROM (clock_timestamp() - v_start_date));
    Exception WHEN OTHERS THEN
      v_err := v_err + 1;
      v_msg := 'Error in step: ' || v_step || ' - ' || SQLERRM;
      RAISE EXCEPTION '%', v_msg;
  END;
  END;

  BEGIN
  DECLARE
      v_start_date timestamp  := clock_timestamp();
    BEGIN
    v_step := 'load bronze.crm_prd_info';
    TRUNCATE TABLE bronze.crm_prd_info;
    COPY bronze.crm_prd_info
      FROM '/datasets/source_crm/prd_info.csv'
      WITH (FORMAT csv, HEADER true);
    v_ok := v_ok + 1;
    RAISE NOTICE 'Step [%] completed successfully (%.0f s)',
             v_step,
             extract(epoch FROM (clock_timestamp() - v_start_date));
    Exception WHEN OTHERS THEN
      v_err := v_err + 1;
      v_msg := 'Error in step: ' || v_step || ' - ' || SQLERRM;
      RAISE EXCEPTION '%', v_msg;
  END;
  END;
  
  BEGIN
  DECLARE
      v_start_date timestamp  := clock_timestamp();
    BEGIN
    v_step := 'load bronze.crm_sales_details';
    TRUNCATE TABLE bronze.crm_sales_details;
    COPY bronze.crm_sales_details
      FROM '/datasets/source_crm/sales_details.csv'
      WITH (FORMAT csv, HEADER true);
    v_ok := v_ok + 1;
    RAISE NOTICE 'Step [%] completed successfully (%.0f s)',
             v_step,
             extract(epoch FROM (clock_timestamp() - v_start_date));
    Exception WHEN OTHERS THEN
      v_err := v_err + 1;
      v_msg := 'Error in step: ' || v_step || ' - ' || SQLERRM;
      RAISE EXCEPTION '%', v_msg;
  END;
  END;
  
  BEGIN
  DECLARE
      v_start_date timestamp  := clock_timestamp();
    BEGIN
    v_step := 'load bronze.erp_cust_az12'; 
    TRUNCATE TABLE bronze.erp_cust_az12;
    COPY bronze.erp_cust_az12
      FROM '/datasets/source_erp/CUST_AZ12.csv'
      WITH (FORMAT csv, HEADER true);
    v_ok := v_ok + 1;
    RAISE NOTICE 'Step [%] completed successfully (%.0f s)',
             v_step,
             extract(epoch FROM (clock_timestamp() - v_start_date));
    Exception WHEN OTHERS THEN
      v_err := v_err + 1;
      v_msg := 'Error in step: ' || v_step || ' - ' || SQLERRM;
      RAISE EXCEPTION '%', v_msg;
  END;
  END;

  BEGIN
  DECLARE
      v_start_date timestamp  := clock_timestamp();
    BEGIN
    v_step := 'load bronze.erp_loc_a101';
    TRUNCATE TABLE bronze.erp_loc_a101;
    COPY bronze.erp_loc_a101
      FROM '/datasets/source_erp/LOC_A101.csv'    
      WITH (FORMAT csv, HEADER true);
    v_ok := v_ok + 1;
    RAISE NOTICE 'Step [%] completed successfully (%.0f s)',
             v_step,
             extract(epoch FROM (clock_timestamp() - v_start_date));
    Exception WHEN OTHERS THEN
      v_err := v_err + 1;
      v_msg := 'Error in step: ' || v_step || ' - ' || SQLERRM;
      RAISE EXCEPTION '%', v_msg;
  END;
  END;
  
  BEGIN
  DECLARE
      v_start_date timestamp  := clock_timestamp();
    BEGIN
    v_step := 'load bronze.erp_px_cat_g1v2';
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    COPY bronze.erp_px_cat_g1v2
      FROM '/datasets/source_erp/PX_CAT_G1V2.csv'    
      WITH (FORMAT csv, HEADER true);
    v_ok := v_ok + 1;
    RAISE NOTICE 'Step [%] completed successfully (%.0f s)',
             v_step,
             extract(epoch FROM (clock_timestamp() - v_start_date));
    Exception WHEN OTHERS THEN
      v_err := v_err + 1;
      v_msg := 'Error in step: ' || v_step || ' - ' || SQLERRM;
      RAISE EXCEPTION '%', v_msg;
  END;
  END;
  v_end_batch := clock_timestamp();
  RAISE NOTICE '[%] load bronze finished', to_char(v_end_batch, 'HH24:MI:SS');

  RAISE NOTICE 'load bronze finished in [%] seconds', extract(epoch FROM (v_end_batch - v_start_batch));
END;
$$;

GRANT EXECUTE ON PROCEDURE bronze.load_bronze TO PUBLIC;

GRANT USAGE ON SCHEMA bronze TO PUBLIC;

CALL bronze.load_bronze ();