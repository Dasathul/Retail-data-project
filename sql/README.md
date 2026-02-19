# SQL Scripts – Retail Data Project

This folder contains all SQL scripts used for inspecting, cleaning, transforming, and modeling the Retail Dataset into analytical 360 tables.

---

## 📌 Objective

The purpose of these scripts is to convert raw transactional retail data into structured analytical datasets (Customer 360, Store 360, Order 360) that can be used for business insights and reporting.

---

## 📂 Workflow & Execution Order

### 01_data_inspection.sql

* Initial exploration of raw tables
* Row counts and schema validation
* Identification of null values and duplicates
* Basic sanity checks

### 02_data_cleaning.sql

* Handling missing values
* Removing duplicate records
* Standardizing data formats
* Preparing clean base tables for transformation

### 03_customer360.sql

* Creates the **Customer 360 table**
* Aggregates transactions at customer level
* Calculates lifetime value (LTV)
* Computes purchase frequency and recency metrics
* Builds a consolidated customer analytics dataset

### 04_store360.sql

* Creates the **Store 360 table**
* Aggregates store-level revenue and order metrics
* Calculates performance KPIs
* Enables store performance comparison

### 05_order360.sql

* Creates the **Order 360 table**
* Builds order-level summary dataset
* Calculates basket size and order metrics
* Enables transaction-level analysis

### 06_eda.sql

* Exploratory Data Analysis (EDA)
* Revenue trends over time
* Category and product performance
* Key business insights queries

---

## 🛠 SQL Concepts Used

* Aggregations (SUM, COUNT, AVG)
* GROUP BY analysis
* Window functions
* Joins
* Subqueries
* Derived tables

---

## 📊 Outcome

These scripts transform raw retail data into structured analytical 360 tables that support:

* Customer segmentation
* Revenue analysis
* Store performance tracking
* Order-level insights
* Business decision-making
