-- =============================================
-- Object: Silver Table
-- Layer: Silver
-- Description:
-- This table stores data after cleaning and transformation
-- from the bronze layer.
--
-- Data in this layer is:
-- - Cleaned (removed nulls, fixed formats)
-- - Standardized (consistent values like gender, country)
-- - Deduplicated (no repeated records)
--
-- Purpose:
-- To prepare reliable and structured data
-- for building analytical models in the gold layer.
-- =============================================



use datawarehouse;



if object_id('silver.crm_cust_info','U') is not null
	drop table silver.crm_cust_info;

create table silver.crm_cust_info 
(
	cst_id	int ,
	cst_key	nvarchar(50),
	cst_firstname nvarchar(50),
	cst_lastname nvarchar(50),
	cst_marital_status nvarchar(50),
	cst_gndr nvarchar(50),
	cst_create_date date,
dwh_create_data DATETIME2 default GETDATE()
)

if object_id('silver.crm_prd_info ','U') is not null
	drop table silver.crm_prd_info;


create table silver.crm_prd_info 
(
prd_id	int ,
cat_id nvarchar(50),
prd_key nvarchar(50),

prd_nm	nvarchar(50),
prd_cost int,
prd_line nvarchar(50),
prd_start_dt datetime,
prd_end_dt datetime,
dwh_create_data DATETIME2 DEFAULT GETDATE()
)

if object_id('silver.crm_sales_details','U') is not null
	drop table silver.crm_sales_details ;

create table silver.crm_sales_details 
(
sls_ord_num	NVARCHAR(50),
sls_prd_key	NVARCHAR(50),
sls_cust_id	INT,
sls_order_dt DATE,
sls_ship_dt	DATE,
sls_due_dt	DATE,
sls_sales	INT,
sls_quantity INT,
sls_price INT 
,
dwh_create_data DATETIME2 DEFAULT GETDATE()
)



if object_id('silver.erp_CUST_AZ12','U') is not null
	drop table silver.erp_CUST_AZ12;

create table silver.erp_CUST_AZ12
(

CID nvarchar(50),
BDATE date,
GEN nvarchar(50)
,
dwh_create_data DATETIME2 DEFAULT GETDATE()
)

if object_id('silver.erp_LOC_A101','U') is not null
	drop table silver.erp_LOC_A101;

create table silver.erp_LOC_A101
(
CID nvarchar(50),
CNTRY nvarchar(50)
,
dwh_create_data DATETIME2 DEFAULT GETDATE()
)


if object_id('silver.erp_PX_CAT_G1V2','U') is not null
	drop table silver.erp_PX_CAT_G1V2;

create table silver.erp_PX_CAT_G1V2
(
ID nvarchar(50),	
CAT	nvarchar(50),
SUBCAT	nvarchar(50),
MAINTENANCE nvarchar(50),
dwh_create_data DATETIME2 DEFAULT GETDATE()

)
