CREATE DATABASE Internship
--------------------------------------------------------------------------------------------------------------------
----------------------------------------------- 01 Data inspection -------------------------------------------------
--------------------------------------------------------------------------------------------------------------------

----------------------------------------------- CUSTOMERS META DATA INSPECTION--

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

----------------------------------------------- ORDERS META DATA INSPECTION--

-- 1 - Checking for duplicates--

SELECT Customer_ID,order_id,product_id,Channel,Delivered_StoreID,Bill_date_timestamp ,COUNT(*) AS DuplicateCount
FROM Orders
GROUP BY Customer_ID,order_id,product_id,Channel,Delivered_StoreID,Bill_date_timestamp
HAVING COUNT(*) > 1;
-- Duplicates records are present--

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

----------------------------------------------- ORDERPAYMENTS META DATA INSPECTION--

-- 1 - Checking for duplicates--

SELECT order_id,payment_type,payment_value,COUNT(*) AS DuplicateCount
FROM OrderPayments
GROUP BY order_id,payment_type,payment_value
HAVING COUNT(*) > 1;

-- Duplicates records are present--

-- 2 - payment value datatype is different. 

-- 3 - Checking for missing values

SELECT
    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS Missing_order_id,
    SUM(CASE WHEN payment_type IS NULL THEN 1 ELSE 0 END) AS Missing_payment_type,
    SUM(CASE WHEN payment_value IS NULL THEN 1 ELSE 0 END) AS Missing_payment_value
FROM OrderPayments;


----------------------------------------------- OrderReview_Ratings META DATA INSPECTION--

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


----------------------------------------------- Productsinfo META DATA INSPECTION--

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

----------------------------------------------- Stores info META DATA INSPECTION--

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

----------------------------------------------- Data discrepency checks--

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


--Converting bill date coloumn into datetime format to check for orders placed before sep 2021 and after oct 2023--
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


--------------------------------------------------------------------------------------------------------------------