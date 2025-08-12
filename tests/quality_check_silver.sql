-- Quality Check for Silver CRM Data
-- Check for duplicate or null customer IDs
-- expected: no results
select cst_id,
count(*) 
from silver.crm_cust_info
group by cst_id
having count(*) > 1 or cst_id is null;

-- Check for Unwanted Spaces
-- Expected: no results
select cst_key 
from silver.crm_cust_info
where cst_key != trim(cst_key);

-- Check for unwanted spaces in customer names
-- expected: no results
select cst_firstname
from silver.crm_cust_info
where cst_firstname != trim(cst_firstname); 

-- Check for unwanted spaces in customer last names
-- expected: no results
select cst_lastname
from silver.crm_cust_info
where cst_lastname != trim(cst_lastname);


-- Data Standardization & consistency
select distinct 
    cst_marital_status
from silver.crm_cust_info;

select distinct cst_gndr
from silver.crm_cust_info;

-- Checking 'silver.crm_prd_info'

-- Check for NULLs or duplicates in Primary Key
-- expected: no results
SELECT 
    prd_id,
    COUNT(*) 
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Check for unwanted spaces in product names
-- expected: no results
select prd_name
from silver.crm_prd_info
where prd_name != trim(prd_name);

-- Check for NULLs or Negative values in product cost
-- expected: no results
select prd_cost
from silver.crm_prd_info
where prd_cost < 0 or prd_cost is null;

-- Data Standardization & consistency
select distinct
    prd_line
from silver.crm_prd_info;

-- Check for Invalid Dates Order
-- expected: no results
select *
from silver.crm_prd_info
where prd_end_dt < prd_start_dt;

-- Checking 'silver.crm_sales_details'

-- Check for Invalid Dates
-- expected: no results
select coalesce(sls_due_dt, 0) as sls_due_dt
from bronze.crm_sales_details
where sls_due_dt <= 0 
    or length((sls_due_dt::text)) != 8
    or sls_due_dt > 20500101
    or sls_due_dt < 19000101;

-- Check for Invalid Date Orders (Order Date > Shipping/Due Dates)
-- Expected: No Results
select *
from silver.crm_sales_details
where sls_ord_dt > sls_ship_dt
    or sls_ord_dt > sls_due_dt;

-- Check Data Consistency: sales = Quantity * Price
-- Expected: No Results
select 
    sls_sales, 
    sls_quantity, 
    sls_price
from silver.crm_sales_details
where sls_sales != sls_quantity * sls_price
    or sls_sales is null
    or sls_quantity is null
    or sls_price is null
    or sls_quantity <= 0
    or sls_price <= 0
    or sls_sales <= 0
order by sls_sales, sls_quantity, sls_price;

-- Checking 'silver.erp_cust_az12'
-- Identify Out-of-Range Dates
-- Expected: Birthdates between 1924 and today

select distinct 
    bdate
from silver.erp_cust_az12
where bdate < '1924-01-01'
    or bdate > current_date;

-- Data Standardization & consistency
select distinct
    gen
from silver.erp_cust_az12;

-- Checking 'silver.erp_loc_a101'
-- Data Standardization & consistency
select distinct
    cntry
from silver.erp_loc_a101
order by cntry;

-- Checking 'silver.erp_px_cat_g1v2'
-- Check for unwanted spaces
-- Expected: no results
select *
from silver.erp_px_cat_g1v2
where cat != trim(cat)
    or subcat != trim(subcat)
    or manteinance != trim(manteinance);

-- Data Standardization & consistency
select distinct
    manteinance
from silver.erp_px_cat_g1v2;