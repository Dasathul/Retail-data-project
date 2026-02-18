--------------------------------------------------------------------------------------------------------------------
----------------------------------------------- 06 Exploratory data analysis ---------------------------------------
--------------------------------------------------------------------------------------------------------------------
-- Some Aggregated metrics --

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


-----------------------------------------------CUSTOMER360 EDA --
SELECT * FROM Customer360
--Customers by state --


SELECT customer_state, COUNT(*) AS customer_count
FROM Customer360
GROUP BY customer_state
ORDER BY customer_count DESC


--Average spend per state --

SELECT customer_state,
       ROUND(AVG(total_amount_spent),2) AS avg_spend,
       ROUND(AVG(total_profit),2) AS avg_profit
FROM Customer360
GROUP BY customer_state
ORDER BY avg_spend DESC


--Gender wise spending --


SELECT Gender,
       ROUND(AVG(total_amount_spent),2) AS avg_spent,
       ROUND(AVG(total_profit),2) AS avg_profit,
       COUNT(*) AS customer_count
FROM Customer360
GROUP BY Gender;



--Average profit and discount per transaction --
SELECT* FROM Customer360

SELECT 
  COUNT(DISTINCT Custid) AS total_cust,
  ROUND(AVG(total_profit / no_of_transactions), 2) AS avg_profit_per_txn,
  ROUND(AVG(total_discount / no_of_transactions), 2) AS avg_discount_per_txn,
  ROUND(AVG(total_profit),2) AS avg_profit_per_cust
FROM Customer360;


--Distribution of preferred payment method --


SELECT preferred_payment_method,
       COUNT(*) AS customer_count,
       COUNT(*)*100.0/(SELECT COUNT(*) FROM Customer360) AS perc
FROM Customer360
GROUP BY preferred_payment_method
ORDER BY customer_count DESC;


--Payment diversity vs spending --


SELECT no_of_different_payment_types,
       ROUND(AVG(total_amount_spent),2) AS avg_spent,
       ROUND(AVG(total_profit),2) AS avg_profit
FROM Customer360
GROUP BY no_of_different_payment_types
ORDER BY no_of_different_payment_types;


--Average tenure, inactivity, and frequency --


SELECT 
  ROUND(AVG(Tenure),1) AS avg_tenure_days,
  ROUND(AVG(Inactive_Days),1) AS avg_inactive_days,
  ROUND(AVG(Frequency),2) AS avg_txn_frequency
FROM Customer360;


--Weekday vs Weekend transactions --


SELECT
  ROUND(AVG(CAST(Transactions_Weekdays AS DECIMAL(10, 2))), 2) AS avg_weekday_txns,
  ROUND(AVG(CAST(Transactions_Weekends AS DECIMAL(10, 2))), 2) AS avg_weekend_txns
FROM 
  Customer360;


--Hourly activity patterns --


SELECT 
  SUM(CASE WHEN Transactions_6AM_12Noon=1 THEN 1 ELSE 0 END) AS morning,
  SUM(CASE WHEN Transactions_12Noon_6PM=1 THEN 1 ELSE 0 END) AS afternoon,
  SUM(CASE WHEN Transactions_6PM_12AM=1 THEN 1 ELSE 0 END) AS evening,
  SUM(CASE WHEN Transactions_12AM_6AM=1 THEN 1 ELSE 0 END) AS night
FROM Customer360;


--Customer Segmentation recency segment --


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


--Customer Segmentation frequency segment --


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


--Customer Segmentation monetary segment --


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


--Combined RFM Summary  --
 

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


-----------------------------------------------ORDER360 EDA --
SELECT* FROM order360
--Basic Summary Metrics --


SELECT 
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(DISTINCT Customer_id) AS total_customers,
    SUM(qty) AS total_quantity_sold,
    ROUND(SUM(amount),2) AS total_revenue,
    ROUND(SUM(total_profit),2) AS total_profit
FROM Order360;


--Average values per order --


SELECT 
    ROUND(AVG(no_of_items),2) AS avg_items_per_order,
    ROUND(AVG(qty),2) AS avg_qty_per_order,
    ROUND(AVG(amount),2) AS avg_order_amount,
    ROUND(AVG(total_profit),2) AS avg_order_profit
FROM Order360;


--% of loss-making orders --


SELECT 
    SUM(CASE WHEN flag_loss_making = 1 THEN 1 ELSE 0 END) AS loss_making_orders,
    COUNT(*) AS total_orders,
    ROUND(100.0 * SUM(CASE WHEN flag_loss_making = 1 THEN 1 ELSE 0 END) / COUNT(*),2) AS pct_loss_making
FROM Order360; 


--Relationship between discount and profit --


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

--Weekend vs Weekday orders --


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


--Order patterns by time of day --


SELECT 
    hours_flag AS time_of_day,
    COUNT(*) AS order_count,
    ROUND(AVG(amount),2) AS avg_order_amount,
    ROUND(AVG(total_profit),2) AS avg_profit
FROM Order360
GROUP BY hours_flag
ORDER BY order_count DESC


--Orders by channel --


SELECT 
    Channel,
    COUNT(*) AS order_count,
    ROUND(AVG(amount),2) AS avg_order_value,
    ROUND(AVG(total_profit),2) AS avg_profit,
    (COUNT(*)*100.0) / (SELECT COUNT(*) FROM Order360) AS perc
FROM Order360
GROUP BY Channel
ORDER BY order_count DESC;


--Orders by payment type --


SELECT 
    payment_type,
    COUNT(*) AS order_count,
    ROUND(AVG(amount),2) AS avg_order_value,
    ROUND(AVG(total_profit),2) AS avg_profit,
    (COUNT(*)*100.0) / (SELECT COUNT(*) FROM Order360) AS perc
FROM Order360
GROUP BY payment_type
ORDER BY order_count DESC;


--Average rating and profit relationship --


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


--Top stores by number of unique customers --


SELECT 
    store_id,
    COUNT(DISTINCT Customer_id) AS unique_customers,
    ROUND(SUM(total_profit),2) AS total_profit,
    (SUM(total_profit)*100.0) / (SELECT SUM(total_profit) FROM Order360) AS perc
FROM Order360
GROUP BY store_id
ORDER BY unique_customers DESC;


--Top customers by total spend --


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


-----------------------------------------------STORE360 EDA --
--High-Profit Stores --


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


--Weekend vs Weekday Sales Comparison --


SELECT 
    SUM(Weekend_sales) AS Weekend_Sales,
    SUM(Weekday_sale) AS Weekday_Sales,
    ROUND(SUM(Weekend_sales)*100.0 / NULLIF(SUM(Weekend_sales + Weekday_sale),0),2) AS Weekend_Sales_Percentage
FROM stores360

ORDER BY Weekend_Sales_Percentage DESC;


--Top 10 Stores by performance Score --


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


-----------------------------------------------CROSS TABLE EDA --
--Gender based order trends --


SELECT 
    c.Gender,
    COUNT(DISTINCT o.order_id) AS Total_Orders,
    ROUND(AVG(o.amount), 2) AS Avg_Order_Value,
    ROUND(SUM(o.total_profit), 2) AS Total_Profit
FROM Customer360 AS c
JOIN Order360 AS o ON c.Custid = o.Customer_id
GROUP BY c.Gender;


--Channel Usage by Customer Type --


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


--Discount Usage by Customer Type --


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


--Recency vs Average Order Value --


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


--MOM Profit --

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


--Category wise revenue --


SELECT 
    p.Category,
    ROUND(SUM(c.Total_Amount),2) AS total_revenue
FROM cleanedOrders AS c
LEFT JOIN Productsinfo AS p
ON c.product_id = p.product_id
GROUP BY p.Category
ORDER BY total_revenue DESC


--discounted orders % --
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


--------------------------------------------------------------------------------------------------------------------