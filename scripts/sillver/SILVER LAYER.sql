USE datawarehouse
GO


CREATE OR ALTER PROCEDURE silver.load_silver AS
    BEGIN
    DECLARE @start_time DATETIME , @end_time DATETIME , @BATCH_START_TIME DATETIME , @BATCH_END_TIME DATETIME
        set @BATCH_START_TIME = GETDATE()
        
        set @start_time = GETDATE()

        TRUNCATE TABLE [silver].[crm_cust_info]
        insert into [silver].[crm_cust_info] (
        [cst_id],
        [cst_key],
        [cst_firstname],
        [cst_lastname],
        [cst_marital_status],
        [cst_gndr],
        [cst_create_date]

        )
        select 
        [cst_id],
        [cst_key],
        trim([cst_firstname]) as [cst_firstname],
        trim([cst_lastname]) as [cst_lastname],
        case
	        when UPPER(TRIM([cst_marital_status])) = 'S' then 'Single'
	        when UPPER(TRIM([cst_marital_status])) = 'M' then 'Married'
	        ELSE 'n/a'
        end as [cst_marital_status],
        case
	        when UPPER(TRIM(cst_gndr)) = 'F' then 'Female'
	        when UPPER(TRIM(cst_gndr)) = 'M' then 'Male'
	        ELSE 'n/a'
        end as cst_gndr,

        [cst_create_date]

        from (

        select *, 
        ROW_NUMBER() OVER(partition by cst_id order by cst_create_date) as flag
        from bronze.crm_cust_info
        where cst_id is not null
        )t

        where flag = 1 ;


        
        SET @end_time = GETDATE()
        PRINT '[silver].[crm_cust_info] load duration: '+ cast(datediff( second ,@end_time , @start_time )as nvarchar)


        
        
        -------------------------


        set @start_time = GETDATE()
        truncate table [silver].[crm_prd_info]
        insert into [silver].[crm_prd_info] (
               [prd_id]
              ,[cat_id]
              ,[prd_key]
              ,[prd_nm]
              ,[prd_cost]
              ,[prd_line]
              ,[prd_start_dt]
              ,[prd_end_dt]
   
        )


        SELECT [prd_id]
              ,REPLACE(SUBSTRING(prd_key , 1 , 5) , '-' , '_') as cat_id 
              ,SUBSTRING(prd_key , 7 , len(prd_key)) as prd_key
              ,[prd_nm]
              ,ISNULL([prd_cost],0) [prd_cost]
              ,
              case UPPER(TRIM(prd_line)) 
              when 'M' then 'Mountain'
              when 'R' then 'Road'
              when 'S' then 'Other Sales'
              when 'T' then 'Touring'
              else 'n/a'
              end as [prd_line]
              ,cast([prd_start_dt] as date) as [prd_start_dt]
              ,cast(lead([prd_start_dt]) over ( partition by  prd_key order by prd_start_dt ) - 1 as date ) as [prd_end_dt]
      
          FROM [bronze].[crm_prd_info]

          
               set @end_time = GETDATE()
               print '[silver].[crm_prd_info] load duration: ' + cast(datediff( second ,@end_time , @start_time )as nvarchar)

          -------------------------


         set @start_time = GETDATE()


        TRUNCATE TABLE [silver].[crm_sales_details]
        insert into [silver].[crm_sales_details] (
            sls_ord_num	    ,
            sls_prd_key	    ,
            sls_cust_id     ,
            sls_order_dt    ,
            sls_ship_dt	    ,
            sls_due_dt	    ,
            sls_sales	    ,
            sls_quantity    ,
            sls_price       

            )

        SELECT [sls_ord_num]
              ,[sls_prd_key]
              ,[sls_cust_id]
              ,
              case 
                WHEN LEN([sls_order_dt]) != 8 or [sls_order_dt] = 0 
                    THEN NULL
                    ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) 
                END AS sls_order_dt 
              ,
              CASE 
                WHEN LEN([sls_ship_dt]) != 8 or [sls_ship_dt] = 0 
              then null
                ELSE CAST(CAST([sls_ship_dt] as varchar) as date) 
                    END AS [sls_ship_dt]
              ,
              CASE 
                WHEN LEN([sls_due_dt]) != 8 or [sls_due_dt] = 0
                    then null
                    ELSE CAST(CAST([sls_due_dt] as varchar) as date) 
                END AS [sls_due_dt]
      

      

              ,CASE 
                WHEN [sls_sales] is null or [sls_sales] <= 0 or [sls_sales] != [sls_quantity] * abs([sls_price])
                      THEN [sls_quantity] * abs([sls_price]) 
                      ELSE [sls_sales]
                   END AS sls_sales


              ,[sls_quantity] 

              ,CASE 
                 WHEN [sls_price] is null or [sls_price] <= 0 
                        THEN [sls_sales] / nullif([sls_quantity],0) 
                        ELSE [sls_price]
                    END AS [sls_price]

          FROM [datawarehouse].[bronze].[crm_sales_details];


          
                set @end_time = GETDATE()
               print '[silver].[erp_PX_CAT_G1V2] load duration: ' + cast(datediff( second ,@end_time , @start_time )as nvarchar)

          -----------------------------

        set @start_time = GETDATE()

        TRUNCATE TABLE [silver].[erp_CUST_AZ12]
        INSERT INTO [silver].[erp_CUST_AZ12] (
        CID   ,
        BDATE ,
        GEN   
        )

        SELECT  
        CASE 
            WHEN [CID] LIKE 'NAS%' THEN SUBSTRING([CID],4,LEN([CID]))
            ELSE [CID]
            END AS [CID]
        ,
        CASE 
            WHEN [BDATE] > GETDATE() THEN NULL 
            ELSE [BDATE] 
            END AS [BDATE]
        ,
        CASE 
            WHEN UPPER(TRIM([GEN])) IN ('MALE' , 'M') THEN 'Male' 
            WHEN UPPER(TRIM([GEN])) IN ('FEMALE' , 'F') THEN 'Female' 
            ELSE 'n/a'
            END AS [GEN] 

        FROM [datawarehouse].[bronze].[erp_CUST_AZ12]
 
 
 
         set @end_time = GETDATE()
         print '[silver].[erp_CUST_AZ12] load duration: ' + cast(datediff( second ,@end_time , @start_time )as nvarchar)

         -------------
  

        set @start_time = GETDATE()
        TRUNCATE TABLE [silver].[erp_LOC_A101]
        INSERT INTO [silver].[erp_LOC_A101] (

        [CID],
        [CNTRY] 

        )

        SELECT 
        REPLACE ([CID],'-','') AS [CID],

        CASE WHEN TRIM(CNTRY) = '' or CNTRY is null THEN 'n/a' 
	        WHEN UPPER(TRIM(CNTRY)) IN ('USA' , 'US') THEN 'United States'	
	        WHEN UPPER(TRIM(CNTRY)) = 'DE' THEN 'Germany'  
	        ELSE CNTRY
            END AS CNTRY

        FROM [datawarehouse].[bronze].[erp_LOC_A101]

               set @end_time = GETDATE()
               print '[silver].[erp_LOC_A101] load duration: ' + cast(datediff( second ,@end_time , @start_time )as nvarchar)

          --------------------

        SET @start_time = GETDATE()
        TRUNCATE TABLE [silver].[erp_PX_CAT_G1V2]
        INSERT INTO [silver].[erp_PX_CAT_G1V2] (
               [ID]
              ,[CAT]
              ,[SUBCAT]
              ,[MAINTENANCE]
              )

               SELECT   replace([ID] , '_' , '-') [ID]
              ,[CAT]
              ,[SUBCAT]
              ,[MAINTENANCE]
      
               FROM [bronze].[erp_PX_CAT_G1V2]
               
               set @end_time = GETDATE()
               print '[silver].[erp_PX_CAT_G1V2] load duration: ' + cast(datediff(second , @end_time , @start_time  )as nvarchar)

               set @BATCH_END_TIME = GETDATE()
               PRINT'THE WHOLE BATCH LOAD DURATION: ' + cast(datediff( second , @BATCH_START_TIME ,  @BATCH_END_TIME )as nvarchar)

    END


