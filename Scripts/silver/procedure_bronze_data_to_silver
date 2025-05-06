/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
		DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME

	BEGIN TRY
		set @batch_start_time  = getdate();
		set @start_time  = getdate();
		print '============================================'
		print 'Loading Silver Layer'
		print '============================================'
		print 'Loading CRM Tables'
		print '============================================'

		-- CRM Customer Info
		print'Truncatinf table : silver.crm_cust_info'
		TRUNCATE TABLE silver.crm_cust_info;
		print'Insert data into silver.crm_cust_info'
		INSERT INTO silver.crm_cust_info (
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date
		)
		SELECT
			cst_id,
			cst_key,
			TRIM(cst_firstname),
			TRIM(cst_lastname), 
			CASE 
				WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				ELSE 'N/A'
			END,
			CASE 
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				ELSE 'N/A'
			END,
			cst_create_date
		FROM (
			SELECT *,
				ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS FLAG_LAST
			FROM bronze.crm_cust_info 
			WHERE cst_id IS NOT NULL
		) t
		WHERE FLAG_LAST = 1;

		SET @end_time = GETDATE();
		print 'Load duration : ' + cast(datediff(second, @start_time, @end_time) AS nvarchar) + 'Seconds'
		print '--------------------------------------------'






				
		set @start_time  = getdate();
		-- CRM Product Info
		print'Truncatinf table : silver.crm_prd_info'
		TRUNCATE TABLE silver.crm_prd_info;
		print'Insert data into silver.crm_prd_info'
		INSERT INTO silver.crm_prd_info(
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)
		SELECT
			prd_id,
			REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_'),
			SUBSTRING(prd_key, 7, LEN(prd_key)),
			prd_nm,
			ISNULL(prd_cost, 0),
			CASE 
				WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'		
				WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'		
				WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'		
				WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
				ELSE 'N/A'
			END,
			prd_start_dt,
			DATEADD(DAY, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt))
		FROM bronze.crm_prd_info;
		SET @end_time = GETDATE();
		print 'Load duration : ' + cast(datediff(second, @start_time, @end_time) AS nvarchar) + 'Seconds'
		print '--------------------------------------------'






		set @start_time  = getdate();
		-- CRM Sales Details
		print'Truncatinf table : silver.crm_sales_details'
		TRUNCATE TABLE silver.crm_sales_details;
		print'Insert data into silver.crm_sales_details'
		INSERT INTO silver.crm_sales_details (
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,	
			sls_price
		)
		SELECT
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE 
				WHEN sls_order_dt = 0 OR LEN(CAST(sls_order_dt AS VARCHAR)) != 8 THEN NULL
				ELSE CAST(STUFF(STUFF(CAST(sls_order_dt AS VARCHAR), 5, 0, '-'), 8, 0, '-') AS DATE)
			END,
			CASE 
				WHEN sls_ship_dt = 0 OR LEN(CAST(sls_ship_dt AS VARCHAR)) != 8 THEN NULL
				ELSE CAST(STUFF(STUFF(CAST(sls_ship_dt AS VARCHAR), 5, 0, '-'), 8, 0, '-') AS DATE)
			END,
			CASE 
				WHEN sls_due_dt = 0 OR LEN(CAST(sls_due_dt AS VARCHAR)) != 8 THEN NULL
				ELSE CAST(STUFF(STUFF(CAST(sls_due_dt AS VARCHAR), 5, 0, '-'), 8, 0, '-') AS DATE)
			END,
			CASE 
				WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price)
					THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales	
			END,
			sls_quantity,
			CASE
				WHEN sls_price <= 0 OR sls_price IS NULL 
					THEN sls_sales / NULLIF(sls_quantity, 0)
				ELSE sls_price
			END
		FROM bronze.crm_sales_details;
		SET @end_time = GETDATE();
		print 'Load duration : ' + cast(datediff(second, @start_time, @end_time) AS nvarchar) + 'Seconds'
		print '--------------------------------------------'




		print 'Loading ERP Tables'
		print '============================================'
		set @start_time  = getdate();
		-- ERP Customer Info
		print'Truncatinf table : silver.erp_CUST_AZ12'
		TRUNCATE TABLE silver.erp_CUST_AZ12;
		print'Insert data into silver.crm_cust_info'
		INSERT INTO silver.erp_CUST_AZ12 (
			cid,
			bdate,
			gen
		)
		SELECT 
			CASE 
				WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
				ELSE cid
			END,
			CASE
				WHEN bdate > GETDATE() THEN NULL
				ELSE bdate
			END,
			CASE 
				WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
				WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
				ELSE 'N/A'
			END
		FROM bronze.erp_CUST_AZ12;
		SET @end_time = GETDATE();
		print 'Load duration : ' + cast(datediff(second, @start_time, @end_time) AS nvarchar) + 'Seconds'
		print '--------------------------------------------'






		set @start_time  = getdate();
		-- ERP Location Info
		print'Truncatinf table : silver.erp_LOC_A101'
		TRUNCATE TABLE silver.erp_LOC_A101;
		print'Insert data into silver.erp_LOC_A101'
		INSERT INTO silver.erp_LOC_A101 (
			cid,
			cntry
		)
		SELECT
			REPLACE(cid, '-', ''),
			CASE 
				WHEN TRIM(cntry) = '' OR TRIM(cntry) IS NULL THEN 'N/A'
				WHEN TRIM(cntry) = 'DE' THEN 'Germany'	
				WHEN TRIM(cntry) = 'US' THEN 'United States'
				ELSE TRIM(cntry)
			END
		FROM bronze.erp_LOC_A101;
		SET @end_time = GETDATE();
		print 'Load duration : ' + cast(datediff(second, @start_time, @end_time) AS nvarchar) + 'Seconds'
		print '--------------------------------------------'






		set @start_time  = getdate();
		-- ERP Product Category
		print'Truncatinf table : silver.erp_PX_CAT_G1V2'
		TRUNCATE TABLE silver.erp_PX_CAT_G1V2;
		print'Insert data into silver.erp_PX_CAT_G1V2'
		INSERT INTO silver.erp_PX_CAT_G1V2 (
			id,
			cat,
			subcat,
			maintenance
		)
		SELECT *
		FROM bronze.erp_PX_CAT_G1V2;
		SET @end_time = GETDATE();
		print 'Load duration : ' + cast(datediff(second, @start_time, @end_time) AS nvarchar) + 'Seconds'
		print '--------------------------------------------'

		set @batch_end_time  = getdate();
		print 'Full Load duration : ' + cast(datediff(second, @batch_start_time, @batch_end_time) AS nvarchar) + 'Seconds'
		print '--------------------------------------------'
	END TRY

	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH

END;


--EXEC silver.load_silver
