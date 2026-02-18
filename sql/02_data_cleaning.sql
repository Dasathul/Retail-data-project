--------------------------------------------------------------------------------------------------------------------
----------------------------------------------- 02 Data cleaning ---------------------------------------------------
--------------------------------------------------------------------------------------------------------------------

-----------------------------------------------droping duplicates in STORES INFO --------------------------
WITH RankedStores AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY seller_city, seller_state, Region
            ORDER BY storeID
        ) AS rn
    FROM [Stores info]
)
DELETE FROM RankedStores
WHERE rn > 1;



-----------------------------------------------taking average of multiple ratings for a single order id in ORDERREVIEW RATINGS --------------------

-- Step 1
SELECT 
    order_id,
    AVG(Customer_Satisfaction_Score) AS Customer_Satisfaction_Score
INTO #Aggregated
FROM [OrderReview Ratings]
GROUP BY order_id;

-- Step 2
DELETE FROM [OrderReview Ratings];

-- Step 3
INSERT INTO [OrderReview Ratings] (order_id, Customer_Satisfaction_Score)
SELECT order_id, Customer_Satisfaction_Score FROM #Aggregated;

SELECT *
FROM [OrderReview Ratings]

-- Settings Category = Others inplace of category = #N/A in PRODUCTSINFO --

SELECT *
FROM Productsinfo

UPDATE Productsinfo
SET Category='Others'
WHERE Category='#N/A' 

-----------------------------------------------Changing the arrangement of ORDERPAYMENTS table and saving as orderpayments pivot --------------------

SELECT *
FROM OrderPayments

SELECT order_id,
    SUM(CASE WHEN payment_type = 'voucher' THEN payment_value ELSE 0 END) AS Voucher,
    SUM(CASE WHEN payment_type = 'UPI/Cash' THEN payment_value ELSE 0 END) AS [UPI/Cash],
    SUM(CASE WHEN payment_type = 'credit_card' THEN payment_value ELSE 0 END) AS Credit_card,
    SUM(CASE WHEN payment_type = 'debit_card' THEN payment_value ELSE 0 END) AS debit_card,
    SUM(payment_value) AS [Total Amount]                             
INTO orderpayments_pivot
FROM OrderPayments
GROUP BY order_id


-----------------------------------------------cleaning orderid with multiple storeid where channel = instore ----------------------------------
-- replace lower amount from store get replaced with high amount store --


WITH ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY order_id
               ORDER BY Total_Amount DESC
           ) AS row_num
    FROM Orders
    WHERE Channel = 'Instore'
      AND order_id IN (
          SELECT order_id
          FROM Orders
          WHERE Channel = 'Instore'
          GROUP BY order_id
          HAVING COUNT(DISTINCT Delivered_StoreID) > 1
      )
)
DELETE FROM ranked
WHERE row_num > 1;


-----------------------------------------------changing the datatype of bill date coloumn from nvarchar to datetime----------------------------------

-- Add a new column with correct type

SELECT*
FROM Orders

ALTER TABLE Orders
ADD Bill_date_timestamp_dt datetime2;

UPDATE Orders
SET Bill_date_timestamp_dt = TRY_CONVERT(datetime2, Bill_date_timestamp);

--  Drop the old column
ALTER TABLE Orders
DROP COLUMN Bill_date_timestamp;

--  Rename new column
EXEC sp_rename 'Orders.Bill_date_timestamp_dt', 'Bill_date_timestamp', 'COLUMN';


-----------------------------------------------Deleting records which have bill date before sep 2021--
SELECT *
FROM Orders
WHERE Bill_date_timestamp < '2021-09-01'
ORDER BY Bill_date_timestamp;

DELETE FROM Orders
WHERE Bill_date_timestamp < '2021-09-01';

SELECT *
FROM Orders

-----------------------------------------------Cumulative count issue solving in ORDERS table ----------------------------------

SELECT Customer_ID,order_id,product_id,Channel,Delivered_StoreID,Bill_date_timestamp ,COUNT(*) AS DuplicateCount
FROM Orders
GROUP BY Customer_ID,order_id,product_id,Channel,Delivered_StoreID,Bill_date_timestamp
HAVING COUNT(*) > 1;



WITH ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY order_id, product_id
               ORDER BY Quantity DESC
           ) AS rn
    FROM Orders
)
SELECT *
FROM ranked
WHERE rn = 1;

--  Create a cleaned version
SELECT *
INTO cleanedOrders
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY order_id, product_id
               ORDER BY Quantity DESC
           ) AS rn
    FROM Orders
) ranked
WHERE rn = 1;

SELECT *
FROM cleanedOrders

ALTER TABLE cleanedOrders DROP COLUMN rn;

-----------------------------------------------CLeaning orders with multiple bill dates---------------------------

WITH FirstDate AS (
    SELECT 
        order_id,
        MIN(Bill_date_timestamp) AS First_Bill_date
    FROM Orders
    WHERE Channel = 'Instore'
    GROUP BY order_id
    HAVING COUNT(DISTINCT Bill_date_timestamp) > 1
)

WITH FirstDate AS (
    SELECT 
        order_id,
        MIN(Bill_date_timestamp) AS First_Bill_date
    FROM cleanedOrders
    WHERE Channel = 'Instore'
    GROUP BY order_id
    HAVING COUNT(DISTINCT Bill_date_timestamp) > 1
)
UPDATE O
SET O.Bill_date_timestamp = F.First_Bill_date
FROM cleanedOrders O
JOIN FirstDate F 
    ON O.order_id = F.order_id
WHERE O.Channel = 'Instore';

SELECT *
FROM cleanedOrders

-----------------------------------------------Cleaning orders which is present in orders but not in orderpayments-------------

SELECT DISTINCT o.order_id
FROM (
    SELECT DISTINCT o.order_id
    FROM Orders o
    LEFT JOIN OrderPayments p
      ON o.order_id = p.order_id
    WHERE p.order_id IS NULL
) o
INNER JOIN [OrderReview Ratings] r
  ON o.order_id = r.order_id;

SELECT *
FROM cleanedOrders
WHERE order_id = '005d9a5423d47281ac463a968b3936fb';

DELETE FROM cleanedOrders
WHERE order_id = 'bfbd0f9bdef84302105ad712db648a6c';

-----------------------------------------------cleanng orders where Single order mapped to multiple customers--------

SELECT 
    order_id,
    COUNT(DISTINCT customer_id) AS customer_count
FROM cleanedOrders
GROUP BY order_id
HAVING COUNT(DISTINCT customer_id) > 1;


-- Step 1: Identify orders with multiple customers
WITH MultiCustomerOrders AS (
    SELECT order_id
    FROM cleanedOrders
    GROUP BY order_id
    HAVING COUNT(DISTINCT customer_id) > 1
),

-- Step 2: Identify the top (highest amount) customer for each of those orders
HighValueCustomer AS (
    SELECT 
        o.order_id,
        o.customer_id,
        ROW_NUMBER() OVER (PARTITION BY o.order_id ORDER BY o.total_amount DESC) AS rn
    FROM cleanedOrders o
    INNER JOIN MultiCustomerOrders m
        ON o.order_id = m.order_id
)

-- Step 3: Update all records of that order to use the top customer_id
UPDATE o
SET o.customer_id = h.customer_id
FROM cleanedOrders o
INNER JOIN HighValueCustomer h
    ON o.order_id = h.order_id
WHERE h.rn = 1;


SELECT TOP 2*
FROM cleanedOrders
SELECT TOP 2*
FROM orderpayments_pivot

SELECT COUNT(DISTINCT order_id) AS unique_order_count
FROM cleanedOrders;
-----------------------------------------------changing the mismatching orders into another table
-- checking for mismatch in total amount of cleanedorders and orderpayments pivot(where the abs difference is greater than 1)---

 
WITH OrderTotals AS (
    SELECT 
        order_id,
        SUM(Total_Amount) AS Order_Total
    FROM cleanedOrders
    GROUP BY order_id
)
SELECT 
    o.order_id,
    o.Order_Total,
    ISNULL(p.[Total Amount], 0) AS Payment_Total,
    ROUND(o.Order_Total - ISNULL(p.[Total Amount], 0), 2) AS Difference
FROM OrderTotals o
LEFT JOIN orderpayments_pivot p
    ON o.order_id = p.order_id
WHERE ABS(o.Order_Total - ISNULL(p.[Total Amount], 0)) > 1


--checking for matching records


WITH OrderTotals AS (
    SELECT 
        order_id,
        SUM(total_amount) AS Order_Total
    FROM cleanedOrders
    GROUP BY order_id
)
SELECT 
    o.order_id,
    o.Order_Total,
    ISNULL(p.[Total Amount], 0) AS Payment_Total,
    ROUND(o.Order_Total - ISNULL(p.[Total Amount], 0), 2) AS Difference
FROM OrderTotals o
LEFT JOIN orderpayments_pivot p
    ON o.order_id = p.order_id
WHERE ABS(o.Order_Total - ISNULL(p.[Total Amount], 0)) <= 1


--now changing these mismatching orders into another table

SELECT TOP (0) *
INTO cleanedOrders_Mismatch
FROM cleanedOrders;

SELECT *
FROM cleanedOrders_Mismatch


;WITH OrderTotals AS (
    SELECT 
        order_id,
        SUM(total_amount) AS Order_Total
    FROM cleanedOrders
    GROUP BY order_id
),
MismatchedOrders AS (
    SELECT 
        o.order_id
    FROM OrderTotals o
    LEFT JOIN orderpayments_pivot p
        ON o.order_id = p.order_id
    WHERE ABS(o.Order_Total - ISNULL(p.[Total Amount], 0)) > 1
)
INSERT INTO cleanedOrders_Mismatch
SELECT c.*
FROM cleanedOrders c
INNER JOIN MismatchedOrders m
    ON c.order_id = m.order_id;

SELECT *
FROM cleanedOrders_Mismatch

--Deleting those records from cleaned orders

;WITH OrderTotals AS (
    SELECT 
        order_id,
        SUM(total_amount) AS Order_Total
    FROM cleanedOrders
    GROUP BY order_id
),
MismatchedOrders AS (
    SELECT 
        o.order_id
    FROM OrderTotals o
    LEFT JOIN orderpayments_pivot p
        ON o.order_id = p.order_id
    WHERE ABS(o.Order_Total - ISNULL(p.[Total Amount], 0)) > 1
)
DELETE c
FROM cleanedOrders c
INNER JOIN MismatchedOrders m
    ON c.order_id = m.order_id;

SELECT *
FROM Orders
SELECT*
FROM cleanedOrders_Mismatch


--Move mismatched records from orderpayments_pivot to a mismatch table

SELECT *
INTO orderpayments_mismatch
FROM orderpayments_pivot
WHERE order_id IN (
    SELECT order_id 
    FROM cleanedOrders_mismatch
);

SELECT* FROM orderpayments_mismatch

--Deleting those records from main table

DELETE FROM orderpayments_pivot
WHERE order_id IN (
    SELECT order_id 
    FROM cleanedOrders_mismatch
);


--------------------------------------------------------------------------------------------------------------------