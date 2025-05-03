/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/






CREATE OR ALTER PROCEDURE bronze.load_bronze as
BEGIN
	BEGIN try
			DECLARE @START_TIME DATETIME, @END_TIME DATETIME;
			print('=====================================================')
			PRINT('LOADING BRONZE LAYER');
			print('=====================================================')
			print('Source CRM tables')
			print('------------------------------------------------')


			SET @START_TIME = GETDATE();

			SET @START_TIME = GETDATE();
			PRINT('Truncating table : bronze.crm_cust_info ');
			TRUNCATE TABLE bronze.crm_cust_info
			PRINT('Inserting data into : bronze.crm_cust_info ')
			BULK INSERT bronze.crm_cust_info 
			FROM 'C:\Users\vasud\OneDrive\Documents\MY_DATAWARE_HOUSE_PROJECT_FILES\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
			WITH( 
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);
			SET @END_TIME = GETDATE();

			PRINT '>>Load duration : ' + cast(datediff(second,@START_TIME, @END_TIME ) as nvarchar) + 'seconds'
			print('------------------------------------------------')
				
				

			SET @START_TIME = GETDATE();
			PRINT('Truncating table : bronze.crm_prd_info  ');
			TRUNCATE TABLE bronze.crm_prd_info 
			PRINT('Inserting data into : bronze.crm_prd_info ')
			BULK INSERT bronze.crm_prd_info 
			FROM 'C:\Users\vasud\OneDrive\Documents\MY_DATAWARE_HOUSE_PROJECT_FILES\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
			WITH( 
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);
			SET @END_TIME = GETDATE();			
			PRINT '>>Load duration : ' + cast(datediff(second,@START_TIME, @END_TIME ) as nvarchar) + 'seconds'
			print('------------------------------------------------')


			
			SET @START_TIME = GETDATE();
			PRINT('Truncating table :bronze.crm_sales_details ');
			TRUNCATE TABLE bronze.crm_sales_details
			PRINT('Inserting data into : bronze.crm_sales_details')
			BULK INSERT bronze.crm_sales_details
			FROM 'C:\Users\vasud\OneDrive\Documents\MY_DATAWARE_HOUSE_PROJECT_FILES\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
			WITH( 
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);			
			SET @END_TIME = GETDATE();			
			PRINT '>>Load duration : ' + cast(datediff(second,@START_TIME, @END_TIME ) as nvarchar) + 'seconds'			
			



			print('=====================================================')
			print('Source ERP tables')
			print('=====================================================')
			PRINT('Truncating table : bronze.erp_CUST_AZ12 ');
			SET @START_TIME = GETDATE();
			TRUNCATE TABLE bronze.erp_CUST_AZ12
			PRINT('Inserting data into : bronze.erp_CUST_AZ12 ')
			BULK INSERT bronze.erp_CUST_AZ12
			FROM 'C:\Users\vasud\OneDrive\Documents\MY_DATAWARE_HOUSE_PROJECT_FILES\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
			WITH( 
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);			
			SET @END_TIME = GETDATE();			
			PRINT '>>Load duration : ' + cast(datediff(second,@START_TIME, @END_TIME ) as nvarchar) + 'seconds'
			print('------------------------------------------------')
			
			
			
			SET @START_TIME = GETDATE();
			PRINT('Truncating table : bronze.erp_LOC_A101');
			TRUNCATE TABLE bronze.erp_LOC_A101
			PRINT('Inserting data into : bronze.erp_LOC_A101 ')
			BULK INSERT bronze.erp_LOC_A101
			FROM 'C:\Users\vasud\OneDrive\Documents\MY_DATAWARE_HOUSE_PROJECT_FILES\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
			WITH( 
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);			
			SET @END_TIME = GETDATE();			
			PRINT '>>Load duration : ' + cast(datediff(second,@START_TIME, @END_TIME ) as nvarchar) + 'seconds'
			print('------------------------------------------------')
			
			

			
			SET @START_TIME = GETDATE();
			PRINT('Truncating table : bronze.erp_PX_CAT_G1V2 ');
			TRUNCATE TABLE bronze.erp_PX_CAT_G1V2
			PRINT('Inserting data into : bronze.erp_PX_CAT_G1V2 ')
			BULK INSERT bronze.erp_PX_CAT_G1V2
			FROM 'C:\Users\vasud\OneDrive\Documents\MY_DATAWARE_HOUSE_PROJECT_FILES\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
			WITH( 
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);			
			SET @END_TIME = GETDATE();			
			PRINT '>>Load duration : ' + cast(datediff(second,@START_TIME, @END_TIME ) as nvarchar) + 'seconds'			
			print('------------------------------------------------')



			SET @END_TIME = GETDATE();				
			PRINT 'Loading Bronze Layer is completed successfully!!!'
			PRINT '>>Bronze Layer Total Load duration : ' + cast(datediff(second,@START_TIME, @END_TIME ) as nvarchar) + 'seconds'			
			print('------------------------------------------------')
			
	


		END TRY

		BEGIN CATCH
			PRINT '==================================';
			PRINT 'ERROR OCCUR DURING LOADING BRONZE LAYER';
			PRINT 'ERROR MESSAGE : ' +  ERROR_MESSAGE();
			PRINT 'ERROR MESSAGE : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
			PRINT 'ERROR MESSAGE : ' + CAST(ERROR_STATE() AS NVARCHAR);
			PRINT '==================================' 
		END CATCH;
END
