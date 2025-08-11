create or replace procedure silver.load_silver()
language plpgsql
security definer
as $$
declare
  v_step text;
  v_start_batch timestamp := clock_timestamp();
  v_end_batch timestamp;
  v_ok int := 0;
  v_err int := 0;
  v_msg text;
begin
  raise notice '[%] load silver started', to_char(clock_timestamp(), 'HH24:MI:SS');
  begin
    declare
      v_start_date timestamp := clock_timestamp();
    begin
      v_step := 'load silver.crm_cust_info';
      
      
      -- Load and transform customer information
      truncate TABLE silver.crm_cust_info;
      insert into silver.crm_cust_info (
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_marital_status,
        cst_gndr,
        cst_create_date
        ) 
        select
          cst_id, 
          trim(cst_key), 
          trim(cst_firstname) as cst_firstname, 
          trim(cst_lastname) as cst_lastname, 
          case upper(trim(cst_marital_status)) 
            WHEN 'S' THEN 'Single' 
            WHEN 'M' THEN 'Married' 
            else 'n/a'
          END as cst_marital_status, 
          case upper(trim(cst_gndr)) 
            WHEN 'M' THEN 'Male' 
            WHEN 'F' THEN 'Female' 
            else 'n/a'
          END as cst_gndr,
          cst_create_date 
        from (
          select *, 
            ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
          FROM bronze.crm_cust_info)
        WHERE flag_last = 1;
      v_ok := v_ok + 1;
      raise notice 'Step [%] completed successfully (%.0f s)',
        v_step,
        extract(epoch FROM (clock_timestamp() - v_start_date));
    exception when others then
      v_err := v_err + 1;
      v_msg := 'Error in step: ' || v_step || ' - ' || sqlerrm;
      raise exception '%', v_msg;
    end;
  end;

  begin
    declare
      v_start_date timestamp := clock_timestamp();
    begin
      v_step := 'load silver.crm_prd_info';
      
      
      -- Load and transform product information
      truncate table silver.crm_prd_info;
      insert into silver.crm_prd_info(
        prd_id,
        cat_id,
        prd_key,
        prd_name,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt
        ) 
        SELECT prd_id,
          replace(substring(trim(prd_key), 1, 5), '-','_') as cat_id,
          substring(prd_key,7, length(prd_key)) as prd_key,
          prd_name,
          coalesce(prd_cost, 0) as prd_cost,
          case upper(trim(prd_line))
            when 'M' then 'Mountain'
            when 'R' then 'Road'
            when 'S' then 'Other sales'
            when 'T' then 'Touring'
            else 'n/a'
            end as prd_line,
          prd_start_dt,
          lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)-1 as prd_end_dt
        FROM bronze.crm_prd_info;
    
      v_ok := v_ok + 1;
      raise notice 'Step [%] completed successfully (%.0f s)',
        v_step,
        extract(epoch FROM (clock_timestamp() - v_start_date));
    exception when others then
      v_err := v_err + 1;
      v_msg := 'Error in step: ' || v_step || ' - ' || sqlerrm;
      raise exception '%', v_msg;
    end;
  end;
  
  begin
    declare
      v_start_date timestamp := clock_timestamp();
    begin
      v_step := 'load silver.crm_sales_details';
      
      
      -- Load and transform sales details
      truncate table silver.crm_sales_details;
      insert into silver.crm_sales_details (
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_ord_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price
      ) 
      select sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        CASE
        WHEN sls_ord_dt IS NULL
          OR sls_ord_dt = 0
          OR length(sls_ord_dt::text)!= 8
        THEN NULL
        ELSE to_date(TRIM(sls_ord_dt::text), 'YYYYMMDD')
      END AS sls_ord_dt,
        case 
          when sls_ship_dt is null
          or sls_ship_dt = 0
          or length(sls_ship_dt::text) != 8        
            then null
          else to_date(TRIM(sls_ship_dt::text), 'YYYYMMDD')
        end as sls_ship_dt,
        case
          when sls_due_dt is null 
          or sls_due_dt = 0
          or length(sls_due_dt::text) != 8 
          then null
          else to_date(sls_due_dt::text, 'YYYYMMDD')
        end as sls_due_dt,
        case 
          when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * abs(sls_price) 
          then sls_quantity * abs(sls_price)
          else sls_sales
        end as sls_sales,
        sls_quantity,
      case 
        when sls_price is null 
          or sls_price < 0 
          or sls_price = 0 
        then sls_sales / abs(sls_quantity)
        when sls_price < 0 
          then abs(sls_price)
        else sls_price
      end as sls_price
      from bronze.crm_sales_details; 
    
      v_ok := v_ok + 1;
      raise notice 'Step [%] completed successfully (%.0f s)',
        v_step,
        extract(epoch FROM (clock_timestamp() - v_start_date));
    exception when others then
      v_err := v_err + 1;
      v_msg := 'Error in step: ' || v_step || ' - ' || sqlerrm;
      raise exception '%', v_msg;
    end;
  end;

  -- ERP cleaning Data

 begin
    declare
      v_start_date timestamp := clock_timestamp();
    begin
      v_step := 'load silver.erp_cust_az12';
      
      -- Load and transform ERP customer data
      truncate table silver.erp_cust_az12;
      insert into silver.erp_cust_az12 (cid, bdate, gen) 
      select 
        case 
          when cid like 'NAS%' then substring(cid, 4, length(cid))
          else cid
        end as cid,
        case 
          when bdate > current_date then null
          else bdate
        end as bdate,
        case 
          when upper(trim(gen)) in ('F', 'FEMALE') then 'Female'
          when upper(trim(gen)) in ('M', 'MALE') then 'Male'
          else 'n/a'
        end as gen
      from bronze.erp_cust_az12;
      v_ok := v_ok + 1;
      raise notice 'Step [%] completed successfully (%.0f s)',
        v_step,
        extract(epoch FROM (clock_timestamp() - v_start_date));
    exception when others then
      v_err := v_err + 1;
      v_msg := 'Error in step: ' || v_step || ' - ' || sqlerrm;
      raise exception '%', v_msg;
    end;
  end;

   begin
    declare
      v_start_date timestamp := clock_timestamp();
    begin
      v_step := 'load silver.erp_loc_a101';
      
      -- Load and transform ERP location data
      truncate table silver.erp_loc_a101;
      insert into silver.erp_loc_a101 select 
        replace (cid, '-', '') as cid,
        case 
          when trim(cntry) in ('USA','US') then 'United States'
          when trim(cntry) = 'CAN' then 'Canada'
          when trim(cntry) = 'DE' then 'Germany'
          when trim(cntry) = '' or trim(cntry) is null then 'n/a'
          else trim(cntry)
        end as cntry
      from bronze.erp_loc_a101;
      v_ok := v_ok + 1;
      raise notice 'Step [%] completed successfully (%.0f s)',
        v_step,
        extract(epoch FROM (clock_timestamp() - v_start_date));
    exception when others then
      v_err := v_err + 1;
      v_msg := 'Error in step: ' || v_step || ' - ' || sqlerrm;
      raise exception '%', v_msg;
    end;
  end;

 begin
    declare
      v_start_date timestamp := clock_timestamp();
    begin
      v_step := 'load silver.erp_px_cat_g1v2';
      
      -- Load and transform ERP product category data
      truncate table silver.erp_px_cat_g1v2;
      insert into silver.erp_px_cat_g1v2 (
        id,
        cat,
        subcat,
        manteinance
      )
      select 
        id,
        cat,
        subcat,
        manteinance
      from bronze.erp_px_cat_g1v2;
      v_ok := v_ok + 1;
      raise notice 'Step [%] completed successfully (%.0f s)',
        v_step,
        extract(epoch FROM (clock_timestamp() - v_start_date));
    exception when others then
      v_err := v_err + 1;
      v_msg := 'Error in step: ' || v_step || ' - ' || sqlerrm;
      raise exception '%', v_msg;
    end;
  end;
  v_end_batch := clock_timestamp();
  
  raise notice 'load silver finished in [%] seconds', extract(epoch from (v_end_batch - v_start_batch));
  raise notice '[%] load silver started', to_char(clock_timestamp(), 'HH24:MI:SS');
end;
$$

grant execute on procedure silver.load_silver() to public;

grant usage on schema silver to public;
grant select on all tables in schema silver to public;

call silver.load_silver();