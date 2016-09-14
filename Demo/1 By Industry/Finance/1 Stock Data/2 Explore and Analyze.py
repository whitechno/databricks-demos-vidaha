# Databricks notebook source exported at Wed, 14 Sep 2016 16:57:35 UTC
# MAGIC %md
# MAGIC 
# MAGIC # Data Exploration and Analyis of Stock Data

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Goal 1: Look at some basic characteristics of this dataset to understand it.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Look at the schema of our table.

# COMMAND ----------

# MAGIC %sql describe stock_data

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Figure out how many distinct tickers they are, the min and max date of our data set.

# COMMAND ----------

# MAGIC %sql 
# MAGIC select 
# MAGIC   format_number(count(*), 0) as total_rows,
# MAGIC   format_number(count(distinct(ticker)), 0) as num_tickers,
# MAGIC   min(date) as min_date,
# MAGIC   max(date) as max_date
# MAGIC from 
# MAGIC   stock_data

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Calculate which stocks have the highest average trading volume in the last year.

# COMMAND ----------

# MAGIC %sql 
# MAGIC select 
# MAGIC   ticker, avg(volume) 
# MAGIC from 
# MAGIC   stock_data
# MAGIC where
# MAGIC   date >= "2016-01-01" 
# MAGIC group by 
# MAGIC   ticker
# MAGIC order by
# MAGIC   avg(volume) desc
# MAGIC limit
# MAGIC   10

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC **Results of SQL queries can be displayed in graphs with just a few clicks of a button.**

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   ticker, date, open
# MAGIC from 
# MAGIC   stock_data
# MAGIC where
# MAGIC   date >= "2016-01-01" and ticker in ("AAPL", "MSFT", "GOOG")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Goal 2: Join the stock data set with supplementary information on the companies.
# MAGIC 
# MAGIC The stock_data table contains the ticker symbol, but not information such as:
# MAGIC   * The name of the company
# MAGIC   * The exchange it's traded on.
# MAGIC   * What industry that company is in.
# MAGIC   * The market cap.
# MAGIC   
# MAGIC Let's download that data and join with our table so we have addition information found on this page: http://www.nasdaq.com/screening/company-list.aspx

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Download all the data and copy it to DBFS.

# COMMAND ----------

# MAGIC %sh wget "http://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nasdaq&render=download" -O /tmp/nasdaq

# COMMAND ----------

# MAGIC %sh wget "http://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nyse&render=download" -O /tmp/nyse

# COMMAND ----------

# MAGIC %sh wget "http://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=amex&render=download" -O /tmp/amex

# COMMAND ----------

# MAGIC %sh mkdir -p /DemoData/stock_data/company_list

# COMMAND ----------

# MAGIC %fs cp file:/tmp/nasdaq /DemoData/stock_data/company_list/exchange=nasdaq/company_info.csv

# COMMAND ----------

# MAGIC %fs cp file:/tmp/nyse /DemoData/stock_data/company_list/exchange=nyse/company_info.csv

# COMMAND ----------

# MAGIC %fs cp file:/tmp/amex /DemoData/stock_data/company_list/exchange=amex/company_info.csv

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Now we can create a partitioned CSV table on this data, where the partitions are the exchanges.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS stock_company_info;
# MAGIC 
# MAGIC CREATE TABLE stock_company_info
# MAGIC USING com.databricks.spark.csv
# MAGIC OPTIONS (
# MAGIC   header = "True",
# MAGIC   path "/DemoData/stock_data/company_list"
# MAGIC )

# COMMAND ----------

# MAGIC %sql select * from stock_company_info

# COMMAND ----------

# MAGIC %sql select exchange, count(*) from stock_company_info group by exchange

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Now, let's do query for the stocks with the highest average trading volume, but **add additonal information about each company.**
# MAGIC 
# MAGIC **Join** the stock_data set with the stock_company info to do that.

# COMMAND ----------

# MAGIC %sql 
# MAGIC select 
# MAGIC   ticker, stock_company_info.name, stock_company_info.exchange, stock_company_info.industry, avg(volume)
# MAGIC from 
# MAGIC   stock_data
# MAGIC LEFT JOIN
# MAGIC   stock_company_info on stock_company_info.symbol = stock_data.ticker
# MAGIC where
# MAGIC   date >= "2016-01-01" 
# MAGIC group by 
# MAGIC   ticker, stock_company_info.name, stock_company_info.exchange, stock_company_info.industry 
# MAGIC order by
# MAGIC   avg(volume) desc
# MAGIC limit
# MAGIC   10

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Count the number of companies for each industry.

# COMMAND ----------

# MAGIC %sql 
# MAGIC select 
# MAGIC   stock_company_info.industry, count(distinct(ticker)) as num_companies
# MAGIC from 
# MAGIC   stock_data
# MAGIC LEFT JOIN
# MAGIC   stock_company_info on stock_company_info.symbol = stock_data.ticker
# MAGIC group by 
# MAGIC   stock_company_info.industry
# MAGIC order by
# MAGIC   num_companies desc