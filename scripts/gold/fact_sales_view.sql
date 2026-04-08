-- =============================================
-- View Name: gold.fact_sales
-- Description: 
-- This view contains the main sales transactions.
-- It combines sales data with product and customer dimensions.
-- Each row represents a single sales order with related details.
-- =============================================



CREATE VIEW gold.fact_sales AS  
SELECT 
       [sls_ord_num]     AS order_number
      ,pr.product_key    
      ,cu.customer_key   
      ,[sls_order_dt]    AS order_date
      ,[sls_ship_dt]     AS shipping_date
      ,[sls_due_dt]      AS due_date
      ,[sls_sales]       AS sales_amount
      ,[sls_quantity]    AS quantity
      ,[sls_price]       AS price
      
  FROM [datawarehouse].[silver].[crm_sales_details] AS sd
  left join [gold].[dim_products] AS pr
  on sd.sls_prd_key = pr.product_number  
  left join [gold].[dim_customer] AS cu
  on sd.sls_cust_id = cu.customer_id
