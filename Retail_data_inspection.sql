CREATE DATABASE Internship

EXEC sp_help Customers
EXEC sp_help OrderPayments
EXEC sp_help [OrderReview Ratings]
EXEC sp_help Orders
EXEC sp_help Productsinfo
EXEC sp_help [Stores info]

DROP TABLE Customers

--CUSTOMERS META DATA INSPECTION--
SELECT *
FROM Customers

-- 1 - Checking for duplicates--

SELECT CustID, COUNT(*) AS DuplicateCount
FROM Customers
GROUP BY CustID
HAVING COUNT(*) > 1;
-- No duplicate records found

-- 2 - Checking for missing values

SELECT
    SUM(CASE WHEN Custid IS NULL THEN 1 ELSE 0 END) AS Missing_Custid,
    SUM(CASE WHEN customer_city IS NULL THEN 1 ELSE 0 END) AS Missing_customer_city,table 
    SUM(CASE WHEN customer_state IS NULL THEN 1 ELSE 0 END) AS Missing_customer_state,
    SUM(CASE WHEN Gender IS NULL THEN 1 ELSE 0 END) AS Missing_Gender
FROM Customers;
-- No missing values found --

-- Table summary--
--data about the customers location and gender.each customer have a unique customerid--


--ORDERS META DATA INSPECTION--
SELECT TOP 10*
FROM Orders
SELECT *
FROM Orders
-- 1 - Checking for duplicates--

SELECT Customer_ID,order_id,product_id,Channel,Delivered_StoreID,Bill_date_timestamp ,COUNT(*) AS DuplicateCount
FROM Orders
GROUP BY Customer_ID,order_id,product_id,Channel,Delivered_StoreID,Bill_date_timestamp
HAVING COUNT(*) > 1;

-- Duplicates records are present--
--possible fix:remove duplicates during cleaning--

-- 2 - Checking for missing values--

SELECT
    SUM(CASE WHEN Customer_id IS NULL THEN 1 ELSE 0 END) AS Missing_Customer_id,
    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS Missing_order_id, 
    SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END) AS Missing_product_id,
    SUM(CASE WHEN Channel IS NULL THEN 1 ELSE 0 END) AS Missing_Channel,
    SUM(CASE WHEN Delivered_StoreID IS NULL THEN 1 ELSE 0 END) AS Missing_Delivered_StoreID,
    SUM(CASE WHEN Bill_date_timestamp IS NULL THEN 1 ELSE 0 END) AS Missing_Bill_date_timestamp,
    SUM(CASE WHEN Quantity IS NULL THEN 1 ELSE 0 END) AS Missing_Quantity,
    SUM(CASE WHEN MRP IS NULL THEN 1 ELSE 0 END) AS Missing_MRP,
    SUM(CASE WHEN Discount IS NULL THEN 1 ELSE 0 END) AS Missing_Discount,
    SUM(CASE WHEN Total_Amount IS NULL THEN 1 ELSE 0 END) AS Missing_Total_Amount
FROM Orders;
-- No missing values found --


SELECT Customer_ID,order_id,product_id,Channel,Delivered_StoreID,Bill_date_timestamp,COUNT(*) AS DuplicateCount
FROM Orders
GROUP BY Customer_ID,order_id,product_id,Channel,Delivered_StoreID,Bill_date_timestamp
HAVING COUNT(*) > 1;

SELECT *
FROM Orders
WHERE order_id='713eda1fb337fff2cccfae60fd0b411e'

--ORDERPAYMENTS META DATA INSPECTION--

SELECT *
FROM OrderPayments

-- 1 - Checking for duplicates--

SELECT order_id,payment_type,payment_value,COUNT(*) AS DuplicateCount
FROM OrderPayments
GROUP BY order_id,payment_type,payment_value
HAVING COUNT(*) > 1;

-- Duplicates records are present--
--possible fix:remove duplicates during cleaning--

-- 2 - payment value datatype is different. 

-- 3 -Checking for missing values

SELECT
    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS Missing_order_id,
    SUM(CASE WHEN payment_type IS NULL THEN 1 ELSE 0 END) AS Missing_payment_type,
    SUM(CASE WHEN payment_value IS NULL THEN 1 ELSE 0 END) AS Missing_payment_value
FROM OrderPayments;


--OrderReview_Ratings META DATA INSPECTION--
select top 20*
from [OrderReview Ratings]

-- 1 - Checking for duplicates--
SELECT order_id,Customer_Satisfaction_Score,COUNT(*) AS DuplicateCount
FROM [OrderReview Ratings]
GROUP BY order_id,Customer_Satisfaction_Score
HAVING COUNT(*) > 1;


SELECT order_id,Customer_Satisfaction_Score
FROM [OrderReview Ratings]
WHERE Customer_Satisfaction_Score>5

--2 checking for missing values
SELECT
    SUM(CASE WHEN Customer_Satisfaction_Score IS NULL THEN 1 ELSE 0 END) AS Missing_Customer_Satisfaction_Score,
    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS Missing_order_id
FROM [OrderReview Ratings]
-- no missing values--


--Productsinfo META DATA INSPECTION--

SELECT top 20*
from Productsinfo

--Checking for missing values--

SELECT
    SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END) AS Missing_product_id ,
    SUM(CASE WHEN Category IS NULL THEN 1 ELSE 0 END) AS Missing_Category, 
    SUM(CASE WHEN product_name_lenght IS NULL THEN 1 ELSE 0 END) AS Missing_product_name_lenght,
    SUM(CASE WHEN product_description_lenght IS NULL THEN 1 ELSE 0 END) AS Missing_product_description_lenght,
    SUM(CASE WHEN product_photos_qty IS NULL THEN 1 ELSE 0 END) AS Missing_product_photos_qty,
    SUM(CASE WHEN product_weight_g IS NULL THEN 1 ELSE 0 END) AS Missing_product_weight_g,
    SUM(CASE WHEN product_length_cm IS NULL THEN 1 ELSE 0 END) AS Missing_product_length_cm,
    SUM(CASE WHEN product_height_cm IS NULL THEN 1 ELSE 0 END) AS Missing_product_height_cm,
    SUM(CASE WHEN product_width_cm IS NULL THEN 1 ELSE 0 END) AS Missing_product_width_cm
FROM Productsinfo;

--checking for duplicate values--

SELECT product_id,Category,COUNT(*) AS DuplicateCount
FROM Productsinfo
GROUP BY product_id,Category
HAVING COUNT(*) > 1;

--Stores info META DATA INSPECTION--

SELECT top 20*
from [Stores info]

--checking for missing values--

SELECT
    SUM(CASE WHEN StoreID IS NULL THEN 1 ELSE 0 END) AS Missing_product_id ,
    SUM(CASE WHEN seller_city IS NULL THEN 1 ELSE 0 END) AS Missing_seller_city, 
    SUM(CASE WHEN seller_state IS NULL THEN 1 ELSE 0 END) AS Missing_seller_state,
    SUM(CASE WHEN Region IS NULL THEN 1 ELSE 0 END) AS Missing_Region
FROM [Stores info]

--checking for duplicate records--
SELECT StoreID,COUNT(*) AS DuplicateCount
FROM [Stores info]
GROUP BY StoreID
HAVING COUNT(*) > 1;

--Data discrepency checks--

--checking for distinct orderids which is present in orderpayments but not in orders--

SELECT DISTINCT o.order_id
FROM Orders o
LEFT JOIN OrderPayments p
  ON o.order_id = p.order_id
WHERE p.order_id IS NULL;

--checking for distinct orderids which is present in orders but not in orderpayments--

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


--checking if multiple productids exist for same order id and customer id--

SELECT Customer_id,order_id,COUNT(DISTINCT product_id) AS noofproducts
FROM Orders
GROUP BY Customer_id,order_id
HAVING COUNT(DISTINCT product_id)>1


--This helps you find records that cannot be interpreted as valid dates.--
SELECT Bill_date_timestamp
FROM Orders
WHERE ISDATE(Bill_date_timestamp) = 0;

SELECT Bill_date_timestamp
FROM Orders
WHERE ISDATE(Bill_date_timestamp) = 1
  AND DAY(TRY_CONVERT(DATETIME, Bill_date_timestamp, 101)) <= 12;


--category wise count of orders--

SELECT 
    category AS Category,
    COUNT(*) AS count_category
FROM ProductsInfo
GROUP BY category
ORDER BY count_category DESC;

--Region wise count of sotres--
SELECT Region, COUNT(*) AS store_count
FROM [Stores info]
GROUP BY Region
ORDER BY store_count DESC;


--orders with multiple customer ids--
SELECT 
    order_id,
    COUNT(DISTINCT customer_id) AS unique_customers
FROM orders
GROUP BY order_id
HAVING COUNT(DISTINCT customer_id) > 1;

--orderids with multiple storeids--
SELECT 
    order_id,
    Channel,
    COUNT(DISTINCT Delivered_StoreID) AS store_count
FROM Orders
WHERE Channel='Instore'
GROUP BY order_id,Channel
HAVING COUNT(DISTINCT Delivered_StoreID) > 1;

--orderids with multiple bill dates--
SELECT 
    order_id,
    Channel,
    COUNT(DISTINCT bill_date_timestamp) AS bill_date_count
FROM cleanedOrders
WHERE Channel='Instore'
GROUP BY order_id,Channel
HAVING COUNT(DISTINCT bill_date_timestamp) > 1;

--orderids where mrp is eqaul to cost per unit--
SELECT *
FROM Orders
WHERE MRP=Cost_Per_Unit

SELECT TOP 10*
FROM Orders
SELECT *
FROM OrderPayments

--Checking if Total amount and payment value is matching--
SELECT *
FROM Orders
WHERE order_id='0097f0545a302aafa32782f1734ff71c'

SELECT *
FROM OrderPayments
WHERE order_id='0097f0545a302aafa32782f1734ff71c'

--checking for duplicate records in orderpayments--
SELECT order_id,payment_type,payment_value,COUNT(*) AS duplicatecount
FROM OrderPayments
GROUP BY order_id,payment_type,payment_value
HAVING COUNT(*)>1

--checking for records where Total Amount in orders table not matching with payment value in orderpayments--
SELECT 
    DISTINCT o.order_id,
    o.Total_Amount AS tot_Amount,
    p.payment_value AS payment_total,
    (o.Total_Amount - p.payment_value) AS difference
FROM Orders o
JOIN OrderPayments p 
    ON o.order_id = p.order_id
WHERE o.Total_Amount <> p.payment_value;





WITH latest_records AS (
    SELECT
        o.*
    FROM Orders o
    INNER JOIN (
        SELECT
            order_id,
            product_id,
            MAX(Quantity) AS max_quantity
        FROM Orders
        GROUP BY order_id, product_id
    ) q
    ON o.order_id = q.order_id
    AND o.product_id = q.product_id
    AND o.Quantity = q.max_quantity
)
SELECT *
FROM latest_records;

--Checking for matching and not matching records(in terms of total amount and payment value)--

WITH latest_records AS (
    SELECT
        o.*
    FROM Orders o
    INNER JOIN (
        SELECT
            order_id,
            product_id,
            MAX(Quantity) AS max_quantity
        FROM Orders
        GROUP BY order_id, product_id
    ) q
    ON o.order_id = q.order_id
    AND o.product_id = q.product_id
    AND o.Quantity = q.max_quantity
),
order_totals AS (
    SELECT 
        order_id,
        SUM(Total_Amount) AS total_order_value
    FROM latest_records
    GROUP BY order_id
),
payment_totals AS (
    SELECT 
        order_id,
        SUM(payment_value) AS total_payment_value
    FROM OrderPayments
    GROUP BY order_id
),
final_comparison AS (
    SELECT 
        o.order_id,
        o.total_order_value,
        p.total_payment_value,
        CASE 
            WHEN ABS(o.total_order_value - COALESCE(p.total_payment_value, 0)) < 0.01 THEN 'MATCH'
            ELSE 'MISMATCH'
        END AS match_status
    FROM order_totals o
    LEFT JOIN payment_totals p 
        ON o.order_id = p.order_id
)
SELECT
    match_status,
    COUNT(*) AS order_count
FROM final_comparison
GROUP BY match_status;

--COonverting bill date coloumn into datetime format to check for orders placed before sep 2021 and after oct 2023--

SELECT *
FROM Orders
WHERE TRY_CONVERT(datetime, Bill_date_timestamp, 1) < '2021-09-01';

SELECT *
FROM Orders
WHERE TRY_CONVERT(datetime, Bill_date_timestamp, 1) > '2023-10-31';

--Looking for orderids which is present in orders but not in customers--
SELECT *
FROM Orders AS o
LEFT JOIN Customers AS c
ON o.Customer_id= c.Custid
WHERE c.Custid IS NULL

SELECT c.Custid
FROM Orders AS o
LEFT JOIN Customers AS c
ON o.Customer_id = c.Custid
WHERE o.Customer_id IS NULL;

SELECT *
FROM Orders

--Checking for distinct customer ids for same order ids--
SELECT order_id, COUNT(DISTINCT Customer_id) AS no_of_customers
FROM Orders
GROUP BY order_id
HAVING COUNT(DISTINCT Customer_id)>1



-----------------------------------------------      CLEANING       -------------------------------------------------


-------------------------- droping duplicates in STORES INFO --------------------------
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



--------------------------- taking average of multiple ratings for a single order id in ORDERREVIEW RATINGS --------------------

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

--Settings Category = Others inplace of category=#N/A in PRODUCTSINFO --

SELECT *
FROM Productsinfo

UPDATE Productsinfo
SET Category='Others'
WHERE Category='#N/A' 

--------------------------Changing the arrangement of ORDERPAYMENTS table and saving as orderpayments pivot --------------------

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


----------------------------------- CLEANING  ORDERID WITH MULTIPLE STOREID WHERE CHANNEL = INSTORE----------------------------------
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


-------------------------------- changing the datatype of bill date coloumn from nvarchar to datetime----------------------------------

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


-- Deleting records which have bill date before sep 2021--
SELECT *
FROM Orders
WHERE Bill_date_timestamp < '2021-09-01'
ORDER BY Bill_date_timestamp;

DELETE FROM Orders
WHERE Bill_date_timestamp < '2021-09-01';

SELECT *
FROM Orders

---------------------------------- Cumulative count issue solving in ORDERS table ----------------------------------

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

-----------------------------------CLeaning orders with multiple bill dates---------------------------

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

--------------------------Cleaning orders which is present in orders but not in orderpayments-------------

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

------------------------------cleanng orders where Single order mapped to multiple customers--------

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

SELECT*
FROM orderpayments_pivot





SELECT TOP 2*
FROM cleanedOrders
SELECT TOP 2*
FROM orderpayments_pivot

SELECT COUNT(DISTINCT order_id) AS unique_order_count
FROM cleanedOrders;



--checking for mismatch in total amount of cleanedorders and orderpayments pivot(where the abs difference is greater than 1)---

 
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


--------------------checking for matching records


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


------------------ now changing these mismatching orders into another table

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

------------------Deleting those records from cleaned orders 

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


------------ Move mismatched records from orderpayments_pivot to a mismatch table

SELECT *
INTO orderpayments_mismatch
FROM orderpayments_pivot
WHERE order_id IN (
    SELECT order_id 
    FROM cleanedOrders_mismatch
);

SELECT* FROM orderpayments_mismatch

------------------------------Deleting those records from main table

DELETE FROM orderpayments_pivot
WHERE order_id IN (
    SELECT order_id 
    FROM cleanedOrders_mismatch
);
---------------------------------- CREATING CUSTOMER 360 ---------------------------------------

WITH base_data AS (
    SELECT
        c.Custid,
        c.customer_city,
        c.customer_state,
        c.Gender,
        o.order_id,
        o.product_id,
        o.Channel,
        o.Delivered_StoreID AS store_id,
        o.Quantity,
        o.Cost_Per_Unit,
        o.MRP,
        o.Discount,
        o.Total_Amount,
        o.Bill_date_timestamp,
        p.Category
    FROM cleanedOrders o
    INNER JOIN Customers c ON o.Customer_id = c.Custid
    LEFT JOIN Productsinfo p ON o.product_id = p.product_id
),

payment_summary AS (
    SELECT
        order_id,
        SUM(Voucher) AS total_voucher,
        SUM([UPI/Cash]) AS total_upi,
        SUM(Credit_card) AS total_credit,
        SUM(debit_card) AS total_debit
    FROM orderpayments_pivot
    GROUP BY order_id
),

order_data AS (
    SELECT
        b.Custid,
        b.customer_city,
        b.customer_state,
        b.Gender,
        b.order_id,
        b.store_id,
        b.Channel,
        b.Category,
        MIN(b.Bill_date_timestamp) AS First_Transaction_Date,
        MAX(b.Bill_date_timestamp) AS Last_Transaction_Date,
        COUNT(DISTINCT b.order_id) AS Frequency,
        SUM(b.Total_Amount) AS total_amount,
        SUM((b.MRP - b.Cost_Per_Unit) * b.Quantity - b.Discount) AS profit,
        SUM(b.Discount) AS total_discount,
        MAX(ps.total_voucher) AS total_voucher,
        MAX(ps.total_upi) AS total_upi,
        MAX(ps.total_credit) AS total_credit,
        MAX(ps.total_debit) AS total_debit,
        CASE WHEN SUM(b.Discount) > 0 THEN 1 ELSE 0 END AS has_discount,
        CASE WHEN SUM((b.MRP - b.Cost_Per_Unit) * b.Quantity - b.Discount) < 0 THEN 1 ELSE 0 END AS has_loss,
        AVG(r.Customer_Satisfaction_Score) AS avg_rating_per_order,
        
        -- 🕓 Time-based flags for each order
        SUM(CASE WHEN DATEPART(WEEKDAY, b.Bill_date_timestamp) IN (1,7) THEN 1 ELSE 0 END) AS is_weekend,
        SUM(CASE WHEN DATEPART(WEEKDAY, b.Bill_date_timestamp) NOT IN (1,7) THEN 1 ELSE 0 END) AS is_weekday,
        CASE WHEN DATEPART(HOUR, b.Bill_date_timestamp) BETWEEN 6 AND 11 THEN 1 ELSE 0 END AS is_6AM_12Noon,
        CASE WHEN DATEPART(HOUR, b.Bill_date_timestamp) BETWEEN 12 AND 17 THEN 1 ELSE 0 END AS is_12Noon_6PM,
        CASE WHEN DATEPART(HOUR, b.Bill_date_timestamp) BETWEEN 18 AND 23 THEN 1 ELSE 0 END AS is_6PM_12AM,
        CASE WHEN DATEPART(HOUR, b.Bill_date_timestamp) BETWEEN 0 AND 5 THEN 1 ELSE 0 END AS is_12AM_6AM
    FROM base_data b
    LEFT JOIN payment_summary ps ON b.order_id = ps.order_id
    LEFT JOIN [OrderReview Ratings] r ON b.order_id = r.order_id
    GROUP BY
        b.Custid, b.customer_city, b.customer_state, b.Gender,
        b.order_id, b.store_id, b.Channel, b.Category, b.Bill_date_timestamp,
        ps.total_voucher, ps.total_upi, ps.total_credit, ps.total_debit
),

order_with_payment AS (
    SELECT *,
        CASE
            WHEN total_voucher >= total_upi AND total_voucher >= total_credit AND total_voucher >= total_debit THEN 'Voucher'
            WHEN total_upi >= total_voucher AND total_upi >= total_credit AND total_upi >= total_debit THEN 'UPI/Cash'
            WHEN total_credit >= total_voucher AND total_credit >= total_upi AND total_credit >= total_debit THEN 'Credit Card'
            ELSE 'Debit Card'
        END AS payment_type
    FROM order_data
),

max_date_cte AS (
    SELECT MAX(Bill_date_timestamp) AS max_date FROM cleanedOrders
),

customer_agg AS (
    SELECT
        owp.Custid,
        MAX(owp.customer_city) AS customer_city,
        MAX(owp.customer_state) AS customer_state,
        MAX(owp.Gender) AS Gender,
        COUNT(DISTINCT owp.order_id) AS no_of_transactions,
        SUM(owp.total_amount) AS total_amount_spent,
        SUM(owp.profit) AS total_profit,
        SUM(owp.total_discount) AS total_discount,
        SUM(owp.has_discount) AS no_of_transactions_with_discount,
        SUM(owp.has_loss) AS no_of_transactions_with_loss,
        COUNT(DISTINCT owp.Channel) AS no_of_channel_used,
        COUNT(DISTINCT owp.store_id) AS no_of_distinct_stores_purchased,
        COUNT(DISTINCT owp.Category) AS no_of_distinct_categories_purchased,
        COUNT(DISTINCT owp.payment_type) AS no_of_different_payment_types,
        SUM(CASE WHEN owp.payment_type = 'Voucher' THEN 1 ELSE 0 END) AS transactions_voucher,
        SUM(CASE WHEN owp.payment_type = 'Credit Card' THEN 1 ELSE 0 END) AS transactions_credit_card,
        SUM(CASE WHEN owp.payment_type = 'Debit Card' THEN 1 ELSE 0 END) AS transactions_debit_card,
        SUM(CASE WHEN owp.payment_type = 'UPI/Cash' THEN 1 ELSE 0 END) AS transactions_upi,
        COUNT(owp.avg_rating_per_order) AS no_of_rated_orders,
        AVG(owp.avg_rating_per_order) AS average_customer_rating,

        -- 🆕 Behavioral Dates
        MIN(owp.First_Transaction_Date) AS First_Transaction_Date,
        MAX(owp.Last_Transaction_Date) AS Last_Transaction_Date,
        DATEDIFF(DAY, MIN(owp.First_Transaction_Date), MAX(owp.Last_Transaction_Date)) AS Tenure,
        (SELECT DATEDIFF(DAY, MAX(owp.Last_Transaction_Date), md.max_date) FROM max_date_cte md) AS Inactive_Days,
        COUNT(DISTINCT owp.order_id) AS Frequency,

        -- 🆕 Time-of-day and day-type insights
        SUM(owp.is_weekday) AS Transactions_Weekdays,
        SUM(owp.is_weekend) AS Transactions_Weekends,
        SUM(owp.is_6AM_12Noon) AS Transactions_6AM_12Noon,
        SUM(owp.is_12Noon_6PM) AS Transactions_12Noon_6PM,
        SUM(owp.is_6PM_12AM) AS Transactions_6PM_12AM,
        SUM(owp.is_12AM_6AM) AS Transactions_12AM_6AM

    FROM order_with_payment owp
    GROUP BY owp.Custid
),

preferred_payment AS (
    SELECT Custid, payment_type AS preferred_payment_method
    FROM (
        SELECT
            Custid,
            payment_type,
            ROW_NUMBER() OVER (PARTITION BY Custid ORDER BY COUNT(*) DESC) AS rn
        FROM order_with_payment
        GROUP BY Custid, payment_type
    ) ranked
    WHERE rn = 1
)

SELECT
    ca.*,
    pp.preferred_payment_method
INTO dbo.Customer360
FROM customer_agg ca
LEFT JOIN preferred_payment pp ON ca.Custid = pp.Custid;


SELECT* FROM Customer360
DROP TABLE Customer360

IF COL_LENGTH('dbo.Customer360', 'Customer_Standard') IS NULL
BEGIN
    ALTER TABLE dbo.Customer360
    ADD Customer_Standard VARCHAR(20);
END
GO

SELECT TOP 5 * FROM dbo.Customer360;

UPDATE dbo.Customer360
SET Customer_Standard =
    CASE
        WHEN (total_amount_spent * 1.0 / NULLIF(no_of_transactions, 0)) >= 316 THEN 'Platinum'
        WHEN (total_amount_spent * 1.0 / NULLIF(no_of_transactions, 0)) >= 158 THEN 'Gold'
        WHEN (total_amount_spent * 1.0 / NULLIF(no_of_transactions, 0)) >= 79 THEN 'Silver'
        ELSE 'Bronze'
    END;
GO


--------------------------------- Creating STORE360 --------------------------------------



CREATE TABLE stores360 (
    StoreID NVARCHAR(20) PRIMARY KEY,
    [Location] NVARCHAR(100),
    No_of_items INT,
    Qty INT,
    Amount DECIMAL(12,2),
    Discount DECIMAL(12,2),
    Items_with_discount INT,
    Total_cost DECIMAL(12,2),
    Total_profit DECIMAL(12,2),
    Flag_loss_making CHAR(1),
    Orders_with_high_profit INT,
    Distinct_categories INT,
    Weekend_sales DECIMAL(12,2),
    Weekday_sale DECIMAL(12,2),
    Average_order_value DECIMAL(12,2),
    Average_profit_per_transaction DECIMAL(12,2),
    Average_profit_per_customer DECIMAL(12,2),
    Average_customer_visits DECIMAL(12,2),
    Average_rating_per_customer DECIMAL(4,2)
);


INSERT INTO stores360 (
    StoreID, [Location],
    No_of_items, Qty, Amount, Discount,
    Items_with_discount, Total_cost, Total_profit,
    Flag_loss_making, Orders_with_high_profit, Distinct_categories,
    Weekend_sales, Weekday_sale,
    Average_order_value, Average_profit_per_transaction,
    Average_profit_per_customer, Average_customer_visits,
    Average_rating_per_customer
)
SELECT
    s.StoreID,
    s.seller_city AS [Location],

    COUNT(DISTINCT p.product_id) AS No_of_items,
    SUM(o.Quantity) AS Qty,
    SUM(o.Total_Amount) AS Amount,
    SUM(o.Discount) AS Discount,
    SUM(CASE WHEN o.Discount > 0 THEN 1 ELSE 0 END) AS Items_with_discount,

    SUM(o.Quantity * o.Cost_Per_Unit) AS Total_cost,
    SUM((o.MRP - o.Cost_Per_Unit - o.Discount) * o.Quantity) AS Total_profit,

    CASE 
        WHEN SUM((o.MRP - o.Cost_Per_Unit - o.Discount) * o.Quantity) < 0 
        THEN 'Y' ELSE 'N' 
    END AS Flag_loss_making,

    SUM(CASE 
        WHEN ((o.MRP - o.Cost_Per_Unit - o.Discount) * o.Quantity) > 500 
        THEN 1 ELSE 0 
    END) AS Orders_with_high_profit,

    COUNT(DISTINCT p.Category) AS Distinct_categories,

    SUM(CASE 
        WHEN DATENAME(WEEKDAY, o.Bill_date_timestamp) IN ('Saturday','Sunday') 
        THEN o.Total_Amount ELSE 0 
    END) AS Weekend_sales,

    SUM(CASE 
        WHEN DATENAME(WEEKDAY, o.Bill_date_timestamp) NOT IN ('Saturday','Sunday') 
        THEN o.Total_Amount ELSE 0 
    END) AS Weekday_sale,

    AVG(o.Total_Amount) AS Average_order_value,
    AVG((o.MRP - o.Cost_Per_Unit - o.Discount) * o.Quantity) AS Average_profit_per_transaction,

    (SUM((o.MRP - o.Cost_Per_Unit - o.Discount) * o.Quantity) 
     / COUNT(DISTINCT o.Customer_id)) AS Average_profit_per_customer,

    NULL AS Average_customer_visits,
    AVG(r.Customer_Satisfaction_Score) AS Average_rating_per_customer

FROM cleanedOrders o
JOIN [Stores info] s 
    ON o.Delivered_StoreID = s.StoreID
JOIN ProductsInfo p 
    ON o.product_id = p.product_id
LEFT JOIN [OrderReview Ratings] r 
    ON o.order_id = r.order_id

GROUP BY 
    s.StoreID, s.seller_city;


SELECT *
FROM stores360

SELECT *
FROM [OrderReview Ratings]


---------------------------------  Create Order360 ------------------------------------


SELECT
    o.order_id,

    COUNT(DISTINCT p.product_id) AS no_of_items,                                -- Total unique items in the order
    SUM(o.Quantity) AS qty,                                            -- Total quantity ordered
    SUM(o.Total_Amount) AS amount,                                     -- Total billed amount for the order
    SUM(o.Discount) AS discount,                                       -- Total discount on the order

    SUM(CASE WHEN o.Discount > 0 THEN 1 ELSE 0 END) AS items_with_discount, -- # of discounted items

    SUM(o.Cost_Per_Unit * o.Quantity) AS total_cost,                   -- Total cost of all items
    SUM((o.MRP - o.Cost_Per_Unit) * o.Quantity - o.Discount) AS total_profit, -- Total profit per order

    CASE 
        WHEN SUM((o.MRP - o.Cost_Per_Unit) * o.Quantity - o.Discount) <= 0 THEN 1 ELSE 0
    END AS flag_loss_making,                                           -- 1 if total profit < 0

    CASE 
        WHEN SUM((o.MRP - o.Cost_Per_Unit) * o.Quantity - o.Discount) > 
             1.2 * AVG((o.MRP - o.Cost_Per_Unit) * o.Quantity - o.Discount) 
        THEN 1 ELSE 0 
    END AS orders_with_high_profit,                                    -- 1 if profit > 120% of avg item profit

    COUNT(DISTINCT p.Category) AS distinct_categories,                 -- No. of unique product categories

    CASE 
        WHEN DATENAME(WEEKDAY, o.Bill_date_timestamp) IN ('Saturday', 'Sunday') THEN 1 ELSE 0 
    END AS weekend_trans_flag,                                         -- Weekend order flag

    CASE 
        WHEN DATEPART(HOUR, o.Bill_date_timestamp) BETWEEN 6 AND 11 THEN 'Morning'
        WHEN DATEPART(HOUR, o.Bill_date_timestamp) BETWEEN 12 AND 17 THEN 'Afternoon'
        WHEN DATEPART(HOUR, o.Bill_date_timestamp) BETWEEN 18 AND 22 THEN 'Evening'
        ELSE 'Night'
    END AS hours_flag,                                                 -- Categorized order time

    -- 🔹 Additional Columns
    o.Delivered_StoreID AS store_id,                                   -- Store identifier
    o.Customer_id,                                                     -- Customer link
    o.Channel,                                                         -- Sales channel

    -- 🟢 Dominant Payment Type
    CASE 
        WHEN pay.Voucher >= pay.[UPI/Cash] AND pay.Voucher >= pay.Credit_card AND pay.Voucher >= pay.debit_card THEN 'Voucher'
        WHEN pay.[UPI/Cash] >= pay.Voucher AND pay.[UPI/Cash] >= pay.Credit_card AND pay.[UPI/Cash] >= pay.debit_card THEN 'UPI/Cash'
        WHEN pay.Credit_card >= pay.Voucher AND pay.Credit_card >= pay.[UPI/Cash] AND pay.Credit_card >= pay.debit_card THEN 'Credit Card'
        ELSE 'Debit Card'
    END AS payment_type,                                               -- Derived dominant payment method

    AVG(r.Customer_Satisfaction_Score) AS avg_rating,                  -- Average rating for the order

    CASE 
        WHEN AVG(r.Customer_Satisfaction_Score) < 3 THEN 'Low'
        WHEN AVG(r.Customer_Satisfaction_Score) BETWEEN 3 AND 4 THEN 'Medium'
        ELSE 'High'
    END AS rating_category,                                            -- Rating category (Low/Medium/High)

    -- 🗓️ Extracted Date Column (Newly Added)
    CAST(o.Bill_date_timestamp AS DATE) AS order_date                  -- Added: clean date without time component

INTO dbo.Order360
FROM cleanedOrders o
LEFT JOIN Productsinfo p 
    ON o.product_id = p.product_id
LEFT JOIN orderpayments_pivot pay
    ON o.order_id = pay.order_id
LEFT JOIN [OrderReview Ratings] r 
    ON o.order_id = r.order_id

GROUP BY
    o.order_id,
    o.Delivered_StoreID,
    o.Customer_id,
    o.Channel,
    pay.Voucher,
    pay.[UPI/Cash],
    pay.Credit_card,
    pay.debit_card,
    o.Bill_date_timestamp;


DROP TABLE Order360
SELECT*
--------------------------------------- Exploratory data analysis -----------------------

--------------------------------------- Aggregated values -------------------------------
SELECT * FROM cleanedOrders

SELECT
    COUNT(DISTINCT o.order_id) AS Total_orders,
    COUNT(DISTINCT o.Customer_id) AS Total_customers,
    COUNT(DISTINCT o.product_id) AS Total_products,
    COUNT(DISTINCT o.Delivered_StoreID) AS Total_stores,
    COUNT(DISTINCT c.customer_state) AS Total_customer_states,
    COUNT(DISTINCT s.seller_state) AS Total_seller_states,
    ROUND(SUM(o.Total_Amount), 2) AS Total_revenue,
    ROUND(SUM(o.Total_Amount) * 1.0 / COUNT(DISTINCT o.order_id), 2) AS AOV,
    ROUND(SUM(o.Quantity) * 1.0 / COUNT(DISTINCT o.order_id), 2) AS Avg_Order_size,
    ROUND(SUM(o.Total_Amount) / COUNT(DISTINCT o.Customer_id), 2) AS Revenue_Per_Customer,
    ROUND(SUM(o.Total_Amount) / COUNT(DISTINCT o.Delivered_StoreID), 2) AS Revenue_Per_Store,
    ROUND((SUM(o.Total_Amount) - SUM(o.Cost_Per_Unit * o.Quantity)) * 100.0 / SUM(o.Total_Amount), 2) AS [Profit_margin%]
FROM 
    cleanedorders AS o 
INNER JOIN Customers AS c ON o.Customer_id = c.Custid
INNER JOIN orderpayments_pivot AS pay ON o.order_id = pay.order_id
INNER JOIN ProductsInfo AS p ON o.product_id = p.product_id
INNER JOIN [OrderReview Ratings] AS r ON o.order_id = r.order_id
INNER JOIN [Stores info] AS s ON o.Delivered_StoreID = s.StoreID;


------------------------------    CUSTOMER360 EDA    -------------------------------------
SELECT * FROM Customer360
----------------------------------Customers by state -------------------------------------


SELECT customer_state, COUNT(*) AS customer_count
FROM Customer360
GROUP BY customer_state
ORDER BY customer_count DESC


----------------------------------Average spend per state -------------------------------------


SELECT customer_state,
       ROUND(AVG(total_amount_spent),2) AS avg_spend,
       ROUND(AVG(total_profit),2) AS avg_profit
FROM Customer360
GROUP BY customer_state
ORDER BY avg_spend DESC


---------------------------------- Gender wise spending -------------------------------------


SELECT Gender,
       ROUND(AVG(total_amount_spent),2) AS avg_spent,
       ROUND(AVG(total_profit),2) AS avg_profit,
       COUNT(*) AS customer_count
FROM Customer360
GROUP BY Gender;



---------------------------------- Average profit and discount per transaction --------------
SELECT* FROM Customer360

SELECT 
  COUNT(DISTINCT Custid) AS total_cust,
  ROUND(AVG(total_profit / no_of_transactions), 2) AS avg_profit_per_txn,
  ROUND(AVG(total_discount / no_of_transactions), 2) AS avg_discount_per_txn,
  ROUND(AVG(total_profit),2) AS avg_profit_per_cust
FROM Customer360;


---------------------------------- Distribution of preferred payment method -------------------


SELECT preferred_payment_method,
       COUNT(*) AS customer_count,
       COUNT(*)*100.0/(SELECT COUNT(*) FROM Customer360) AS perc
FROM Customer360
GROUP BY preferred_payment_method
ORDER BY customer_count DESC;


---------------------------------- Payment diversity vs spending ------------------------------


SELECT no_of_different_payment_types,
       ROUND(AVG(total_amount_spent),2) AS avg_spent,
       ROUND(AVG(total_profit),2) AS avg_profit
FROM Customer360
GROUP BY no_of_different_payment_types
ORDER BY no_of_different_payment_types;


---------------------------------- Average tenure, inactivity, and frequency ------------------------------


SELECT 
  ROUND(AVG(Tenure),1) AS avg_tenure_days,
  ROUND(AVG(Inactive_Days),1) AS avg_inactive_days,
  ROUND(AVG(Frequency),2) AS avg_txn_frequency
FROM Customer360;


---------------------------------- Weekday vs Weekend transactions ------------------------------


SELECT
  ROUND(AVG(CAST(Transactions_Weekdays AS DECIMAL(10, 2))), 2) AS avg_weekday_txns,
  ROUND(AVG(CAST(Transactions_Weekends AS DECIMAL(10, 2))), 2) AS avg_weekend_txns
FROM 
  Customer360;


---------------------------------- Hourly activity patterns ------------------------------


SELECT 
  SUM(CASE WHEN Transactions_6AM_12Noon=1 THEN 1 ELSE 0 END) AS morning,
  SUM(CASE WHEN Transactions_12Noon_6PM=1 THEN 1 ELSE 0 END) AS afternoon,
  SUM(CASE WHEN Transactions_6PM_12AM=1 THEN 1 ELSE 0 END) AS evening,
  SUM(CASE WHEN Transactions_12AM_6AM=1 THEN 1 ELSE 0 END) AS night
FROM Customer360;


---------------------------------- Customer Segmentation recency segment ------------------------------


SELECT 
  CASE 
    WHEN Inactive_Days <= 30 THEN 'Recent'
    WHEN Inactive_Days BETWEEN 31 AND 90 THEN 'Moderate'
    ELSE 'Dormant'
  END AS Recency_Segment,
  COUNT(*) AS Customer_Count
FROM Customer360
GROUP BY 
  CASE 
    WHEN Inactive_Days <= 30 THEN 'Recent'
    WHEN Inactive_Days BETWEEN 31 AND 90 THEN 'Moderate'
    ELSE 'Dormant'
  END
ORDER BY Customer_Count DESC;


---------------------------------- Customer Segmentation frequency segment ------------------------------


SELECT 
  CASE 
    WHEN no_of_transactions >= 10 THEN 'Frequent'
    WHEN no_of_transactions BETWEEN 5 AND 9 THEN 'Moderate'
    ELSE 'Rare'
  END AS Frequency_Segment,
  COUNT(*) AS Customer_Count
FROM Customer360
GROUP BY 
    CASE 
    WHEN no_of_transactions >= 10 THEN 'Frequent'
    WHEN no_of_transactions BETWEEN 5 AND 9 THEN 'Moderate'
    ELSE 'Rare'
  END
ORDER BY Customer_Count DESC;


---------------------------------- Customer Segmentation monetary segment ------------------------------


SELECT 
  CASE 
    WHEN total_amount_spent >= 2000 THEN 'High value'
    WHEN total_amount_spent BETWEEN 500 AND 2000 THEN 'Mid value'
    ELSE 'low value'
  END AS Monetary_Segment,
  COUNT(*) AS Customer_Count
FROM Customer360
GROUP BY 
    CASE 
    WHEN total_amount_spent >= 2000 THEN 'High value'
    WHEN total_amount_spent BETWEEN 500 AND 2000 THEN 'Mid value'
    ELSE 'low value'
  END
ORDER BY Customer_Count DESC;


---------------------------------- Combined RFM Summary  ------------------------------
 

SELECT 
  CASE 
    WHEN Inactive_Days <= 30 THEN 'Recent'
    WHEN Inactive_Days BETWEEN 31 AND 90 THEN 'Moderate'
    ELSE 'Dormant'
  END AS Recency_Segment,
  CASE 
    WHEN no_of_transactions > 7 THEN 'Frequent'
    WHEN no_of_transactions BETWEEN 2 AND 7 THEN 'Moderate'
    WHEN no_of_transactions =1 THEN 'One time buyer'
  END AS Frequency_Segment,
  CASE 
    WHEN total_amount_spent >= 5000 THEN 'High-Value'
    WHEN total_amount_spent BETWEEN 2000 AND 4999 THEN 'Mid-Value'
    ELSE 'Low-Value'
  END AS Monetary_Segment,
  COUNT(*) AS Customer_Count
FROM Customer360
GROUP BY 
    CASE 
    WHEN Inactive_Days <= 30 THEN 'Recent'
    WHEN Inactive_Days BETWEEN 31 AND 90 THEN 'Moderate'
    ELSE 'Dormant'
  END,
    CASE 
    WHEN no_of_transactions > 7 THEN 'Frequent'
    WHEN no_of_transactions BETWEEN 2 AND 7 THEN 'Moderate'
    WHEN no_of_transactions =1 THEN 'One time buyer'
  END,
    CASE 
    WHEN total_amount_spent >= 5000 THEN 'High-Value'
    WHEN total_amount_spent BETWEEN 2000 AND 4999 THEN 'Mid-Value'
    ELSE 'Low-Value'
  END
ORDER BY Customer_Count DESC;


--------------------------------    ORDER360 EDA      -----------------------------------
SELECT* FROM order360
-------------------------------- Basic Summary Metrics ----------------------------------


SELECT 
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(DISTINCT Customer_id) AS total_customers,
    SUM(qty) AS total_quantity_sold,
    ROUND(SUM(amount),2) AS total_revenue,
    ROUND(SUM(total_profit),2) AS total_profit
FROM Order360;


-------------------------------- Average values per order ----------------------------------


SELECT 
    ROUND(AVG(no_of_items),2) AS avg_items_per_order,
    ROUND(AVG(qty),2) AS avg_qty_per_order,
    ROUND(AVG(amount),2) AS avg_order_amount,
    ROUND(AVG(total_profit),2) AS avg_order_profit
FROM Order360;


-------------------------------- % of loss-making orders ----------------------------------


SELECT 
    SUM(CASE WHEN flag_loss_making = 1 THEN 1 ELSE 0 END) AS loss_making_orders,
    COUNT(*) AS total_orders,
    ROUND(100.0 * SUM(CASE WHEN flag_loss_making = 1 THEN 1 ELSE 0 END) / COUNT(*),2) AS pct_loss_making
FROM Order360; 


-------------------------------- Relationship between discount and profit ----------------------------------


SELECT 
    CASE 
        WHEN discount = 0 THEN 'No Discount'
        WHEN discount BETWEEN 0.01 AND 50 THEN 'Low Discount (0-50)'
        WHEN discount BETWEEN 50.01 AND 150 THEN 'Medium Discount (50-150)'
        ELSE 'High Discount (>150)'
    END AS discount_range,
    COUNT(*) AS order_count,
    ROUND(AVG(total_profit),2) AS avg_profit
FROM Order360
GROUP BY 
    CASE 
        WHEN discount = 0 THEN 'No Discount'
        WHEN discount BETWEEN 0.01 AND 50 THEN 'Low Discount (0-50)'
        WHEN discount BETWEEN 50.01 AND 150 THEN 'Medium Discount (50-150)'
        ELSE 'High Discount (>150)'
    END
ORDER BY avg_profit DESC;


-------------------------------- Weekend vs Weekday orders ----------------------------------


SELECT 
    CASE 
        WHEN weekend_trans_flag = 1 THEN 'Weekend'
        ELSE 'Weekday'
    END AS day_type,
    COUNT(*) AS order_count,
    ROUND(AVG(amount),2) AS avg_order_amount,
    ROUND(AVG(total_profit),2) AS avg_profit
FROM Order360
GROUP BY 
    CASE 
        WHEN weekend_trans_flag = 1 THEN 'Weekend'
        ELSE 'Weekday'
    END;


-------------------------------- Order patterns by time of day ----------------------------------


SELECT 
    hours_flag AS time_of_day,
    COUNT(*) AS order_count,
    ROUND(AVG(amount),2) AS avg_order_amount,
    ROUND(AVG(total_profit),2) AS avg_profit
FROM Order360
GROUP BY hours_flag
ORDER BY order_count DESC


-------------------------------- Orders by channel ----------------------------------


SELECT 
    Channel,
    COUNT(*) AS order_count,
    ROUND(AVG(amount),2) AS avg_order_value,
    ROUND(AVG(total_profit),2) AS avg_profit,
    (COUNT(*)*100.0) / (SELECT COUNT(*) FROM Order360) AS perc
FROM Order360
GROUP BY Channel
ORDER BY order_count DESC;


-------------------------------- Orders by payment type ----------------------------------


SELECT 
    payment_type,
    COUNT(*) AS order_count,
    ROUND(AVG(amount),2) AS avg_order_value,
    ROUND(AVG(total_profit),2) AS avg_profit,
    (COUNT(*)*100.0) / (SELECT COUNT(*) FROM Order360) AS perc
FROM Order360
GROUP BY payment_type
ORDER BY order_count DESC;


-------------------------------- Average rating and profit relationship ----------------------------------


SELECT 
    CASE
        WHEN avg_rating = 5 THEN 'Excellent'
        WHEN avg_rating = 4 THEN 'Good'
        WHEN avg_rating = 3 THEN 'Average'
        WHEN avg_rating = 2 THEN 'Bad'
        WHEN Avg_Rating = 1 THEN 'Poor'
    END AS average_rating_per_customer,
    COUNT(*) AS customer_count,
    ROUND(AVG(total_profit),2) AS avg_profit
FROM Order360
GROUP BY 
    CASE
        WHEN avg_rating = 5 THEN 'Excellent'
        WHEN avg_rating = 4 THEN 'Good'
        WHEN avg_rating = 3 THEN 'Average'
        WHEN avg_rating = 2 THEN 'Bad'
        WHEN Avg_Rating = 1 THEN 'Poor'
    END 
ORDER BY customer_count DESC


-------------------------------- Top stores by number of unique customers ----------------------------------


SELECT 
    store_id,
    COUNT(DISTINCT Customer_id) AS unique_customers,
    ROUND(SUM(total_profit),2) AS total_profit,
    (SUM(total_profit)*100.0) / (SELECT SUM(total_profit) FROM Order360) AS perc
FROM Order360
GROUP BY store_id
ORDER BY unique_customers DESC;


-------------------------------- Top customers by total spend ----------------------------------


SELECT 
    Channel,
    payment_type,
    COUNT(*) AS order_count,
    ROUND(AVG(amount),2) AS avg_order_value,
    ROUND(AVG(total_profit),2) AS avg_profit,
    ROUND(SUM(amount),2) AS tot_revenue
FROM Order360
GROUP BY Channel, payment_type
ORDER BY tot_revenue DESC;


-----------------------------------      STORE360 EDA       ------------------------------------
-------------------------------------- High-Profit Stores --------------------------------------


SELECT  
    StoreID,
    [Location],
    SUM(Total_profit) AS Total_Profit,
    SUM(Total_cost) AS Total_Cost,
    ROUND(SUM(Total_profit) * 100.0 / SUM(Total_cost), 2) AS Profit_Margin_Percentage,
    (SUM(Total_profit)*100.0) / (SELECT SUM(Total_profit) FROM stores360) AS perc
FROM stores360
GROUP BY StoreID,[Location]
ORDER BY Profit_Margin_Percentage DESC;


-------------------------------- Weekend vs Weekday Sales Comparison ----------------------------


SELECT 
    SUM(Weekend_sales) AS Weekend_Sales,
    SUM(Weekday_sale) AS Weekday_Sales,
    ROUND(SUM(Weekend_sales)*100.0 / NULLIF(SUM(Weekend_sales + Weekday_sale),0),2) AS Weekend_Sales_Percentage
FROM stores360

ORDER BY Weekend_Sales_Percentage DESC;


-------------------------------- Top 10 Stores by performance Score ----------------------------


SELECT TOP 10
    StoreID,
    Location,
    SUM(Amount) AS Total_Sales,
    SUM(Total_profit) AS Total_Profit,
    AVG(Average_rating_per_customer) AS Avg_Rating,
    (SUM(Amount)*0.5 + SUM(Total_profit)*0.3 + AVG(Average_rating_per_customer)*0.2) AS Performance_Score
FROM stores360
GROUP BY StoreID, Location
ORDER BY Performance_Score DESC;


--------------------------------       CROSS TABLE EDA      ---------------------------------
---------------------------------- Gender based order trends --------------------------------


SELECT 
    c.Gender,
    COUNT(DISTINCT o.order_id) AS Total_Orders,
    ROUND(AVG(o.amount), 2) AS Avg_Order_Value,
    ROUND(SUM(o.total_profit), 2) AS Total_Profit
FROM Customer360 AS c
JOIN Order360 AS o ON c.Custid = o.Customer_id
GROUP BY c.Gender;


------------------------------------ Channel Usage by Customer Type ---------------------------


SELECT 
    CASE 
        WHEN c.total_amount_spent < 500 THEN 'Low Value'
        WHEN c.total_amount_spent BETWEEN 500 AND 2000 THEN 'Mid Value'
        ELSE 'High Value'
    END AS Customer_Segment,
    o.Channel,
    COUNT(DISTINCT o.order_id) AS No_of_Orders,
    ROUND(SUM(o.amount),2) AS Total_Sales
FROM Customer360 AS c
JOIN Order360 AS o ON c.Custid = o.Customer_id
GROUP BY 
    CASE 
        WHEN c.total_amount_spent < 500 THEN 'Low Value'
        WHEN c.total_amount_spent BETWEEN 500 AND 2000 THEN 'Mid Value'
        ELSE 'High Value'
    END,
    o.Channel
ORDER BY Customer_Segment, Total_Sales DESC;


------------------------------------ Discount Usage by Customer Type ---------------------------


SELECT 
    CASE 
        WHEN c.total_amount_spent < 500 THEN 'Low Value'
        WHEN c.total_amount_spent BETWEEN 500 AND 2000 THEN 'Mid Value'
        ELSE 'High Value'
    END AS Customer_Segment,
    COUNT(*) AS customer_count,
    SUM(o.discount) AS Total_Discount_Availed,
    COUNT(CASE WHEN o.items_with_discount > 0 THEN 1 END) AS Discounted_Orders,
    ROUND(AVG(o.discount), 2) AS Avg_Discount_Per_Order,
    (SUM(o.discount)*100.0) / (SELECT SUM(discount) FROM Order360) AS perc
FROM Customer360 AS c
JOIN Order360 AS o ON c.Custid = o.Customer_id
GROUP BY 
    CASE 
        WHEN c.total_amount_spent < 500 THEN 'Low Value'
        WHEN c.total_amount_spent BETWEEN 500 AND 2000 THEN 'Mid Value'
        ELSE 'High Value'
    END
ORDER BY Total_Discount_Availed DESC;


------------------------------------  Recency vs Average Order Value ---------------------------


SELECT 
    CASE 
        WHEN c.Inactive_Days <= 30 THEN 'Recent'
        WHEN c.Inactive_Days BETWEEN 31 AND 90 THEN 'Moderate'
        ELSE 'Dormant'
    END AS Recency_Segment,
    ROUND(AVG(o.amount), 2) AS Avg_Order_Value,
    ROUND(AVG(o.total_profit), 2) AS Avg_Profit_Per_Order
FROM Customer360 AS c
JOIN Order360 AS o ON c.Custid = o.Customer_id
GROUP BY 
    CASE 
        WHEN c.Inactive_Days <= 30 THEN 'Recent'
        WHEN c.Inactive_Days BETWEEN 31 AND 90 THEN 'Moderate'
        ELSE 'Dormant'
    END
ORDER BY Avg_Order_Value DESC;


-------------------------------------------- MOM Profit ----------------------------------------------


WITH MonthlyRevenue AS (
    SELECT 
        YEAR(order_date) AS Order_Year,
        MONTH(order_date) AS Order_Month,
        SUM(amount) AS Total_Revenue,
        COUNT(DISTINCT Customer_id) AS cust_count
    FROM Order360
    WHERE order_date IS NOT NULL
    GROUP BY YEAR(order_date), MONTH(order_date)
)
SELECT 
    Order_Year,
    DATENAME(MONTH, DATEFROMPARTS(Order_Year, Order_Month, 1)) AS Month_Name,
    ROUND(Total_Revenue, 2) AS tot_revenue,
    cust_count,
    ROUND(LAG(Total_Revenue) OVER (ORDER BY Order_Year, Order_Month), 2) AS Prev_Month_Revenue,
    ROUND(
        (Total_Revenue - LAG(Total_Revenue) OVER (ORDER BY Order_Year, Order_Month)) 
        * 100.0 / NULLIF(LAG(Total_Revenue) OVER (ORDER BY Order_Year, Order_Month), 0),
        2
    ) AS MoM_Growth_Percentage
FROM MonthlyRevenue
ORDER BY Order_Year, Order_Month;


--------------------------------------------- Category wise revenue -------------------------------------


SELECT 
    p.Category,
    ROUND(SUM(c.Total_Amount),2) AS total_revenue
FROM cleanedOrders AS c
LEFT JOIN Productsinfo AS p
ON c.product_id = p.product_id
GROUP BY p.Category
ORDER BY total_revenue DESC


--------------------------------------------- discounted orders % -------------------------------------
SELECT* FROM Order360 WHERE no_of_items=1

SELECT 
    SUM(items_with_discount) AS discounted_orders,
    SUM(items_with_discount)*100.0 / SUM(no_of_items) AS perc
FROM Order360


SELECT order_id,
    COUNT(distinct product_id) AS no_of_prods
FROM cleanedOrders
GROUP BY order_id
HAVING COUNT(distinct product_id)>1



----------------------------------------------------------------------------------------------------


SELECT* FROM Customer360
SELECT* FROM Order360
SELECT* FROM stores360
SELECT* FROM Productsinfo
SELECT* FROM cleanedOrders