/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema).

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Drop and Create View: gold.dim_customers
-- =============================================================================
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key,
    ci.cst_id AS customer_id,
    ci.cst_key AS customer_number,
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,
    la.cntry AS country,
    ci.cst_marital_status AS marital_status,
    CASE 
        WHEN ci.cst_gndr != 'N/A' THEN ci.cst_gndr
        ELSE COALESCE(cb.gen, 'N/A')
    END AS gender,
    cb.bdate AS birthdate,
    ci.cst_create_date AS create_date
FROM silver.crm_cust_info AS ci
JOIN silver.erp_CUST_AZ12 AS cb
    ON ci.cst_key = cb.CID
LEFT JOIN silver.erp_LOC_A101 AS la
    ON ci.cst_key = la.cid;
GO

-- =============================================================================
-- Drop and Create View: gold.dim_products
-- =============================================================================
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products AS
SELECT
    ROW_NUMBER() OVER (ORDER BY pr.prd_start_dt, pr.prd_id) AS product_key,  -- Surrogate key
    pr.prd_id AS product_id,
    pr.prd_key AS product_number,
    pr.prd_nm AS product_name,
    pr.cat_id AS category_id,
    cat.cat AS category,
    cat.subcat AS sub_category,
    cat.maintenance,
    pr.prd_cost AS cost,
    pr.prd_line AS product_line,
    pr.prd_start_dt AS start_date
FROM silver.crm_prd_info AS pr
LEFT JOIN silver.erp_PX_CAT_G1V2 AS cat
    ON pr.cat_id = cat.ID
WHERE pr.prd_end_dt IS NULL;  -- Only current data
GO

-- =============================================================================
-- Drop and Create View: gold.fact_sales
-- =============================================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS
SELECT 
    sa.sls_ord_num AS order_number,
    pr.product_key,
    cu.customer_key,
    sa.sls_order_dt AS order_date,
    sa.sls_ship_dt AS shipping_date,
    sa.sls_due_dt AS due_date,
    sa.sls_sales AS sales_amount,
    sa.sls_quantity AS quantity,
    sa.sls_price AS price
FROM silver.crm_sales_details AS sa
LEFT JOIN gold.dim_products AS pr
    ON sa.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers AS cu
    ON sa.sls_cust_id = cu.customer_id;
GO
