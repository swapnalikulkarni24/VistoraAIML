/* STEP 1: FEATURE ENGINEERING QUERY
This query extracts data from Snowflake sample tables, transforms it,
and engineers features.
*/
WITH customer_orders AS (
    SELECT
        c.C_CUSTKEY,
        c.C_NAME,
        c.C_ACCTBAL,
        o.O_ORDERKEY,
        o.O_TOTALPRICE,
        o.O_ORDERDATE,
        o.O_ORDERPRIORITY
    FROM
        SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER c
    LEFT JOIN
        SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS o
        ON c.C_CUSTKEY = o.O_CUSTKEY
),

engineered_features AS (
    SELECT
        C_CUSTKEY,
        ANY_VALUE(C_NAME) AS CUSTOMER_NAME,
        ANY_VALUE(C_ACCTBAL) AS ACCOUNT_BALANCE,
        
        -- Aggregation Feature
        COUNT(DISTINCT O_ORDERKEY) AS TOTAL_ORDERS,
        
        -- Aggregation Feature
        COALESCE(SUM(O_TOTALPRICE), 0) AS TOTAL_SPEND,
        
        -- Transformation Feature
        CASE
            WHEN TOTAL_ORDERS = 0 THEN 0
            ELSE TOTAL_SPEND / TOTAL_ORDERS
        END AS AVG_SPEND_PER_ORDER,
        
        -- Encoding Feature
        MAX(CASE 
                WHEN O_ORDERPRIORITY = '1-URGENT' THEN 1 
                ELSE 0 
            END) AS HAS_HIGH_PRIORITY_ORDERS
        
    FROM
        customer_orders
    GROUP BY
        C_CUSTKEY
)

SELECT * FROM engineered_features;
/* STEP 2: "LOAD" INTO FEATURE STORE
We create a View, which acts as our simple, real-time Feature Store.
The view doesn't store data, but it stores the *logic* to generate features.
*/
CREATE DATABASE MY_PROJECT_DB;
CREATE OR REPLACE VIEW MY_PROJECT_DB.PUBLIC.CUSTOMER_FEATURE_STORE AS
(
    -- Paste the entire query from above (starting from "WITH customer_orders AS ...")
    WITH customer_orders AS (
        SELECT
            c.C_CUSTKEY,
            c.C_NAME,
            c.C_ACCTBAL,
            o.O_ORDERKEY,
            o.O_TOTALPRICE,
            o.O_ORDERDATE,
            o.O_ORDERPRIORITY
        FROM
            SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER c
        LEFT JOIN
            SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS o
            ON c.C_CUSTKEY = o.O_CUSTKEY
    ),
    
    engineered_features AS (
        SELECT
            C_CUSTKEY,
            ANY_VALUE(C_NAME) AS CUSTOMER_NAME,
            ANY_VALUE(C_ACCTBAL) AS ACCOUNT_BALANCE,
            COUNT(DISTINCT O_ORDERKEY) AS TOTAL_ORDERS,
            COALESCE(SUM(O_TOTALPRICE), 0) AS TOTAL_SPEND,
            CASE
                WHEN TOTAL_ORDERS = 0 THEN 0
                ELSE TOTAL_SPEND / TOTAL_ORDERS
            END AS AVG_SPEND_PER_ORDER,
            MAX(CASE 
                    WHEN O_ORDERPRIORITY = '1-URGENT' THEN 1 
                    ELSE 0 
                END) AS HAS_HIGH_PRIORITY_ORDERS
            
        FROM
            customer_orders
        GROUP BY
            C_CUSTKEY
    )
    
    SELECT * FROM engineered_features
);
