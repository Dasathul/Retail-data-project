--------------------------------------------------------------------------------------------------------------------
----------------------------------------------- 04 Store360 --------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------


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


--------------------------------------------------------------------------------------------------------------------