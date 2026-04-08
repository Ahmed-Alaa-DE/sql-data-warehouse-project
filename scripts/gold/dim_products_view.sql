CREATE VIEW gold.dim_products AS
SELECT 
       row_number() over(order by pn.[prd_start_dt] , pn.prd_key) AS product_KEY
      ,[prd_id] as product_id
      ,pn.[prd_key] as product_number
      ,pn.[prd_nm] as product_name
      ,pn.[cat_id] as category_id

      ,pc.[CAT] as category
      ,pc.[SUBCAT] as sub_category
      ,pc.[MAINTENANCE] 

      ,pn.[prd_cost] as cost
      ,pn.[prd_line] as product_line
      ,pn.[prd_start_dt] as start_date 
      

  FROM [datawarehouse].[silver].[crm_prd_info] as pn
  left join silver.erp_PX_CAT_G1V2 pc
    on pn.cat_id = pc.id
  where prd_end_dt is null 