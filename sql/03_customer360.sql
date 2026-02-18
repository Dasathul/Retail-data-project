--------------------------------------------------------------------------------------------------------------------
----------------------------------------------- 03 customer360 -----------------------------------------------------
--------------------------------------------------------------------------------------------------------------------


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



--------------------------------------------------------------------------------------------------------------------