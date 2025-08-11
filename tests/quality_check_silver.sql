
select cst_id,
count(*) 
from silver.crm_cust_info
group by cst_id
having count(*) > 1 or cst_id is null;

select cst_firstname
from silver.crm_cust_info
where cst_firstname != trim(cst_firstname); 

select cst_lastname
from silver.crm_cust_info
where cst_lastname != trim(cst_lastname);

select distinct cst_gndr
from silver.crm_cust_info;


SELECT 
    prd_id,
    COUNT(*) 
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

select prd_cost
from bronze.crm_prd_info
where prd_cost < 0 or prd_cost is null;

select * from bronze.crm_prd_info
where prd_end_dt < prd_start_dt;