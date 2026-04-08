create view gold.dim_customer as
SELECT 
ROW_NUMBER() OVER(ORDER BY ci.[cst_id]) AS customer_key,

ci.[cst_id] as customer_id
      ,ci.[cst_key] as customer_number 
      ,ci.[cst_firstname] as firs_name 
      ,ci.[cst_lastname] AS last_name
      ,cl.[CNTRY] AS country 
      ,ci.[cst_marital_status] AS marital_status
      
      ,CASE 
            WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr --considering CRM is the master for gender info
            ELSE COALESCE(ca.gen,'n/a')
       END AS gender

      ,ca.[BDATE] AS birth_date
      ,ci.[cst_create_date] AS create_date
      
      
      
      
  FROM [datawarehouse].[silver].[crm_cust_info] ci
  left join [datawarehouse].[silver].[erp_CUST_AZ12] ca
  on ca.[CID] = ci.[cst_key]
  left join [silver].[erp_LOC_A101] cl
  on cl.[CID] = ci.[cst_key]


