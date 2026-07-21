


/*========================================================
Script: Create Bronze Layer Tables
Description: Creates all raw staging tables in the Bronze schema
for CRM and ERP source systems in the Data Warehouse.

This script:
- Drops existing tables if they exist
- Recreates Bronze tables for raw data ingestion
- Prepares tables for BULK INSERT operations

Source Systems:
- CRM
- ERP

Layer: Bronze (Raw Data Layer)
========================================================*/

use datawarehouse;




  -- ################################
-- starting schema : source_crm
      -- ################################
--=============================
ddl for table : bronze.crm_prd_info
--=============================
if object_id('bronze.crm_prd_info','U') is not null
    drop table bronze.crm_prd_info; 
go
create table bronze.crm_prd_info ( 
     prd_id nvarchar(500),
     prd_key nvarchar(500),
     prd_nm nvarchar(500),
     prd_cost nvarchar(500),
     prd_line nvarchar(500),
     prd_start_dt nvarchar(500),
     prd_end_dt nvarchar(500)
);
go

--=============================
ddl for table : bronze.crm_cust_info
--=============================
if object_id('bronze.crm_cust_info','U') is not null
    drop table bronze.crm_cust_info; 
go
create table bronze.crm_cust_info ( 
     cst_id nvarchar(500),
     cst_key nvarchar(500),
     cst_firstname nvarchar(500),
     cst_lastname nvarchar(500),
     cst_marital_status nvarchar(500),
     cst_gndr nvarchar(500),
     cst_create_date nvarchar(500)
);
go

--=============================
ddl for table : bronze.crm_sales_details
--=============================
if object_id('bronze.crm_sales_details','U') is not null
    drop table bronze.crm_sales_details; 
go
create table bronze.crm_sales_details ( 
     sls_ord_num nvarchar(500),
     sls_prd_key nvarchar(500),
     sls_cust_id nvarchar(500),
     sls_order_dt nvarchar(500),
     sls_ship_dt nvarchar(500),
     sls_due_dt nvarchar(500),
     sls_sales nvarchar(500),
     sls_quantity nvarchar(500),
     sls_price nvarchar(500)
);
go



  -- ################################
-- starting schema : source_erp
      -- ################################
--=============================
ddl for table : bronze.erp_CUST_AZ12
--=============================
if object_id('bronze.erp_CUST_AZ12','U') is not null
    drop table bronze.erp_CUST_AZ12; 
go
create table bronze.erp_CUST_AZ12 ( 
     CID nvarchar(500),
     BDATE nvarchar(500),
     GEN nvarchar(500)
);
go

--=============================
ddl for table : bronze.erp_LOC_A101
--=============================
if object_id('bronze.erp_LOC_A101','U') is not null
    drop table bronze.erp_LOC_A101; 
go
create table bronze.erp_LOC_A101 ( 
     CID nvarchar(500),
     CNTRY nvarchar(500)
);
go

--=============================
ddl for table : bronze.erp_PX_CAT_G1V2
--=============================
if object_id('bronze.erp_PX_CAT_G1V2','U') is not null
    drop table bronze.erp_PX_CAT_G1V2; 
go
create table bronze.erp_PX_CAT_G1V2 ( 
     ID nvarchar(500),
     CAT nvarchar(500),
     SUBCAT nvarchar(500),
     MAINTENANCE nvarchar(500)
);
go

