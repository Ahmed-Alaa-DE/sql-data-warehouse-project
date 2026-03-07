/*==============================================================
Script: Bronze Layer Data Load Procedure
Object: bronze.load_bronze

Description:
This stored procedure loads raw data into the Bronze layer
of the Data Warehouse from CRM and ERP CSV source files.

Process:
1. Truncate existing Bronze tables
2. Bulk load data from CSV files
3. Log load duration for each dataset
4. Log total batch execution time
5. Handle errors using TRY...CATCH

Source Systems:
- CRM
- ERP

Layer:
Bronze (Raw Data Ingestion Layer)

Author: Ahmed Alaa
==============================================================*/


CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME , @batch_start_time DATETIME , @batch_end_time DATETIME


    BEGIN TRY
    set @batch_start_time = GETDATE();
        PRINT '====================================='
        PRINT 'Loading Bronze Layer'
        PRINT '====================================='

        SET @start_time = GETDATE();
         
        PRINT 'Loading CRM Customer Info'
        TRUNCATE TABLE bronze.crm_cust_info

        BULK INSERT bronze.crm_cust_info
        FROM 'D:\baraa\all stuff\projects\baraa project\sql-data-warehouse-project-main\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        )

        SET @end_time = GETDATE()
        PRINT  '-----------------'
        PRINT  '>>load duration : ' + CAST(DATEDIFF(second , @start_time ,@end_time )AS VARCHAR) + ' seconds';
        PRINT  '-----------------'












        PRINT 'Loading CRM Product Info'
        TRUNCATE TABLE bronze.crm_prd_info
        
        SET @start_time = GETDATE();
        
        BULK INSERT bronze.crm_prd_info
        FROM 'D:\baraa\all stuff\projects\baraa project\sql-data-warehouse-project-main\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        )

        
        SET @end_time = GETDATE()
        PRINT  '-----------------'
        PRINT  '>>load duration : ' + CAST(DATEDIFF(second , @start_time ,@end_time )AS VARCHAR) + ' seconds';
        PRINT  '-----------------'








        
        SET @start_time = GETDATE();
        
        PRINT 'Loading CRM Sales Details'
        TRUNCATE TABLE bronze.crm_sales_details 

        BULK INSERT bronze.crm_sales_details
        FROM 'D:\baraa\all stuff\projects\baraa project\sql-data-warehouse-project-main\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        )
        
        SET @end_time = GETDATE();
        PRINT  '-----------------'
        PRINT  '>>load duration : ' + CAST(DATEDIFF(second , @start_time ,@end_time )AS VARCHAR) + ' seconds';
        PRINT  '-----------------'













        SET @start_time = GETDATE();
        
        PRINT 'Loading ERP Customer Data'
        TRUNCATE TABLE bronze.erp_CUST_AZ12

        BULK INSERT bronze.erp_CUST_AZ12
        FROM 'D:\baraa\all stuff\projects\baraa project\sql-data-warehouse-project-main\datasets\source_erp\CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        )
        
        SET @end_time = GETDATE()
        PRINT  '-----------------'
        PRINT  '>>load duration : ' + CAST(DATEDIFF(second , @start_time ,@end_time )AS VARCHAR) + ' seconds';
        PRINT  '-----------------'










        SET @start_time = GETDATE();
        
        PRINT 'Loading ERP Location Data'
        TRUNCATE TABLE bronze.erp_LOC_A101

        BULK INSERT bronze.erp_LOC_A101
        FROM 'D:\baraa\all stuff\projects\baraa project\sql-data-warehouse-project-main\datasets\source_erp\LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        )
        
        SET @end_time = GETDATE()
        PRINT  '-----------------'
        PRINT  '>>load duration : ' + CAST(DATEDIFF(second , @start_time ,@end_time )AS VARCHAR) + ' seconds';
        PRINT  '-----------------'







        
        SET @start_time = GETDATE();
        
        PRINT 'Loading ERP Product Category Data'
        TRUNCATE TABLE bronze.erp_PX_CAT_G1V2 

        BULK INSERT bronze.erp_PX_CAT_G1V2
        FROM 'D:\baraa\all stuff\projects\baraa project\sql-data-warehouse-project-main\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        )
        
        SET @end_time = GETDATE()
        PRINT  '-----------------'
        PRINT  '>>load duration : ' + CAST(DATEDIFF(second , @start_time ,@end_time )AS VARCHAR) + ' seconds';
        PRINT  '-----------------'



        PRINT '====================================='
        PRINT 'Bronze Layer Loaded Successfully'
        PRINT '====================================='


        SET @batch_end_time = GETDATE()
        PRINT  '-------------------------------------------------------------'
        PRINT  '>>the whole batch load duration : ' + CAST(DATEDIFF(second , @batch_start_time ,@batch_end_time )AS VARCHAR) + ' seconds';
        PRINT  '-------------------------------------------------------------'

    END TRY

    BEGIN CATCH
        PRINT '====================================='
        PRINT 'Error Occured During Loading Bronze Layer'
        PRINT 'error message' + ERROR_MESSAGE();
        PRINT 'error state' + CAST(ERROR_STATE() AS VARCHAR );
        PRINT 'error number' + CAST(ERROR_NUMBER()AS VARCHAR );
        PRINT '====================================='
    END CATCH 


END


