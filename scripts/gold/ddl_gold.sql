drop view gold.dim_customer;
create view gold.dim_customer as 
select 
        ROW_NUMBER() over (order by cst_id) as customer_key,
        ci.cst_id as customer_id,
        ci.cst_key as customer_number,
        ci.cst_firstname as first_name,
        ci.cst_lastname as last_name,
        ci.cst_marital_status as marital_status,
        case 
        when ci.cst_gndr !='n/a' then ci.cst_gndr 
            else coalesce(ca.gen, 'n/a') 
            end as gender,
        ca.bdate as birth_date,
        ci.cst_create_date as create_date
    from silver.crm_cust_info ci
    left join silver.erp_cust_az12 ca
        on ci.cst_key = ca.cid
    left join silver.erp_loc_a101 la
        on ci.cst_key = la.cid;

select distinct gender from gold.dim_customer;

drop VIEW gold.dim_product;
create view gold.dim_product as
select
    row_number() over (order by pn.prd_start_dt, pn.prd_key) as product_key,
    pn.prd_id as product_id,
    pn.prd_key as product_number,
    pn.prd_name as product_name,
    pn.cat_id as category_id,
    pc.cat as category,
    pc.subcat as subcategory,
    pc.manteinance,
    pn.prd_cost as product_cost,
    pn.prd_line as product_line,
    pn.prd_start_dt as product_start_date
from silver.crm_prd_info pn
left join silver.erp_px_cat_g1v2 pc
    on pn.cat_id = pc.id
where pn.prd_end_dt is null;
drop view gold.fact_sales;
create view gold.fact_sales as
select
    si.sls_ord_num as order_number,
    pr.product_key,
    cu.customer_key,
    si.sls_ord_dt as order_date,
    si.sls_ship_dt as shipphing_date,
    si.sls_sales as sales_amount,
    si.sls_quantity as quantity,
    si.sls_price as price
from silver.crm_sales_details si
left join gold.dim_product pr
    on si.sls_prd_key = pr.product_number
left join gold.dim_customer cu
    on si.sls_cust_id = cu.customer_id;
