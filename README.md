# 🛍 Retail Data Analytics Project

## 📌 Project Overview

This presentation summarizes the end-to-end analytics journey of a multi-store retail business,

starting from raw transactional data inspection to building executive-level insights and dashboards.

The dataset represents a retail company operating across:

* 535 stores
* 19 states
* 534 cities
* Transaction data ranging from February 2020 to October 2023

The data includes customer details, orders, payments, products, store information, and customer satisfaction scores.

The objective was to transform raw, inconsistent data into reliable analytical datasets and uncover business-critical insights that support revenue growth,

operational efficiency, and customer strategy.

---

# 🎯 Business Objective

The primary goals of this analysis were:

1. Ensure data accuracy and integrity through structured inspection and cleaning.

2. Identify inconsistencies that could distort financial and operational reporting.

3. Build structured analytical models (Customer 360, Store 360, Order 360).

4. Deliver actionable business insights through dashboards and executive reporting.

Given the scale of:

  * 99,441 customers

  * 112,650 order records

  * 32,951 products

  * 535 stores

Data quality validation was critical before any meaningful business analysis could be performed.

---

# 🏗 Project Architecture

Raw Data
→ Data Inspection
→ Data Cleaning & Validation
→ SQL 360 Modeling
→ Exploratory Data Analysis
→ Power BI Dashboard
→ Business Insights & Strategic Recommendations

---

# 📂 Project Structure

Retail-data-project/

├── data/
│   ├── raw_data/
│   └── cleaned_data/

├── sql/
│   ├── Inspection scripts
│   ├── Cleaning scripts
│   ├── Customer360
│   ├── Order360
│   └── Store360

├── dashboard/
│   ├── Power BI dashboard (.pbix)
│   └── Screenshots

├── Presenatation/
│   └── Final business presentation

└── README.md

---

# 🧹 Data Cleaning & Validation

Major data quality issues identified and resolved:

* Duplicate cumulative transaction logs
* Order–Payment mismatches (6,145 mismatched records detected)
* Date format inconsistencies
* Foreign key integrity violations
* Multiple customers linked to same order ID
* Split payments requiring pivot transformation
* Billing inconsistencies across stores

Final cleaned dataset retained: **95,204 validated orders**

Audit tables were created to preserve mismatched records for traceability.

---

# 🧠 SQL 360 Data Modeling

## 🔹 Customer360

* Lifetime Value (LTV)
* Recency, Frequency, Monetary segmentation
* Payment behavior profiling
* Satisfaction score integration
* Discount sensitivity tracking

## 🔹 Order360

* Order profitability
* Basket size metrics
* Channel-level analysis
* Time-of-day purchasing behavior
* Loss-making order identification

## 🔹 Store360

* Store-level revenue & profit
* Geographic performance benchmarking
* Weekend vs weekday sales tracking
* Performance scoring model:
  (Revenue × 0.5 + Profit × 0.3 + Avg Rating × 0.2)

---

# 📊 Key Insights & Business Breakthroughs

## 🔍 1️⃣ Customer Base Structure Risk

* 95%+ of customers are **low-value, one-time buyers**
* Only 6 customers qualify as frequent buyers
* Majority of customers are dormant

🚨 **Breakthrough Insight:**
The business is heavily dependent on one-time transactional buyers rather than repeat customers.
This indicates weak retention strategy and high acquisition dependency — a major long-term sustainability risk.

---

## 💳 2️⃣ Discount Dependency Problem

* 40% of orders are discounted
* 91% of total discount value is availed by low-value customers

🚨 **Breakthrough Insight:**
Discounts are primarily attracting low-value, one-time buyers rather than high-value customers.
This suggests discount campaigns may be increasing revenue but not improving customer lifetime value — potentially eroding margins.

---

## 📊 3️⃣ Financial Integrity Concern

* 6,145 mismatched records between Orders and Payments tables
* Amount inconsistencies identified at transaction level

🚨 **Breakthrough Insight:**
There is a significant reconciliation gap between billing and payment systems.
If left unresolved, this can distort financial reporting and revenue recognition accuracy.

This demonstrates strong audit-driven analytical thinking.

---

## 🏪 4️⃣ Revenue Concentration Risk

* 22% of total revenue comes from a single store (ST103)
* 60% of customers are concentrated in Andhra Pradesh

🚨 **Breakthrough Insight:**
Revenue is geographically concentrated, creating operational dependency risk.
Any disruption in key locations could significantly impact total revenue.

---

## 📈 5️⃣ Strong Revenue Growth Momentum

* Consistent month-on-month revenue growth
* August 2023 highest revenue month
* Only minor dips in trend

📊 Indicates strong upward sales momentum.

---

## 🧾 6️⃣ Order Profitability Stability

* Only 0.36% of orders are loss-making
* Majority of transactions are profit-positive

💡 Core pricing model appears stable, but discount allocation needs strategic refinement.

---

## 🛍 7️⃣ Channel & Payment Insights

* 87.9% of orders via In-store channel
* 75% of payments via Credit Card
* Online channel small but higher AOV

💡 Online channel has higher order value potential — opportunity for expansion.

---

# 🛠 Tools & Technologies

* SQL Server (T-SQL)
* Power BI
* Data Validation & Audit Techniques
* Exploratory Data Analysis (EDA)
* Data Modeling (360 views)

---

# 💼 Business Value Delivered

This project demonstrates:

* Data integrity validation skills
* Structured SQL transformation capability
* Analytical segmentation modeling
* Executive-level dashboard creation
* Financial reconciliation analysis
* Risk identification mindset

---

# 🚀 Conclusion

This project goes beyond dashboard creation.

It demonstrates:

✔ Data auditing
✔ Financial reconciliation analysis
✔ Customer lifecycle modeling
✔ Strategic risk identification
✔ Revenue concentration analysis
✔ Discount impact assessment

It simulates a real-world retail analytics engagement suitable for:

* Data Analyst roles
* Business Intelligence roles
* SQL Developer roles
* Analytics Consulting profiles

---

*End-to-end retail analytics case study built with strong data validation and business interpretation focus.*
