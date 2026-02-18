--------------------------------------------------------------------------------------------------------------------
----------------------------------------------- 05 Order360 --------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------


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




--------------------------------------------------------------------------------------------------------------------