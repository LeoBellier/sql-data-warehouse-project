-- Check 'gold.dim_customer'
-- Check for uniqueness of customer_key 
-- expected: 0 rows
select 
    customer_key,
    count(*) as duplicate_count
from gold.dim_customer
group by customer_key
having count(*) > 1;

-- Check 'gold.dim_product'
-- Check for uniqueness of product_key 
-- expected: 0 rows
select 
    product_key,
    count(*) as duplicate_count
from gold.dim_product
group by product_key
having count(*) > 1;

-- Foreing key integrity checks
select * from gold.fact_sales f 
left join gold.dim_customer cu
    on cu.customer_key = f.customer_key
left join gold.dim_product p
    on p.product_key = f.product_key
where cu.customer_key is null or p.product_key is null;