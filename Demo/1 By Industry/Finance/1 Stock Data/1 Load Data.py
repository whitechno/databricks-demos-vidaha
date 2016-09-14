# Databricks notebook source exported at Wed, 14 Sep 2016 16:58:03 UTC
# MAGIC %md
# MAGIC 
# MAGIC # Import Stock Data from Quandl

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 1: Download the data

# COMMAND ----------

import quandl

# COMMAND ----------

dbutils.widgets.text("quandl_api_key", "kNg6oG-tXRn7As_S7Z1i", "Quandl API Key:")

# COMMAND ----------

quandl.ApiConfig.api_key = dbutils.widgets.get("quandl_api_key")

# COMMAND ----------

quandl.bulkdownload("WIKI", download_type="complete", filename="/tmp/WIKI.zip")

# COMMAND ----------

# MAGIC %sh unzip /tmp/WIKI.zip -d /tmp

# COMMAND ----------

dbutils.fs.rm("/DemoData/stock_data", True)

# COMMAND ----------

# MAGIC %sh mkdir -p /dbfs/DemoData/stock_data

# COMMAND ----------

# MAGIC %sh cp /tmp/WIKI_*.csv /dbfs/DemoData/stock_data/

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 2: Load the CSV file as a Spark SQL Table

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC CREATE TEMPORARY TABLE stock_data_csv (
# MAGIC   ticker String,
# MAGIC   date   Date,
# MAGIC   open   Float,
# MAGIC   high   Float,
# MAGIC   low    Float,
# MAGIC   close  Float,
# MAGIC   volume Float,
# MAGIC   ex_dividend Float,
# MAGIC   split_ratio Float,
# MAGIC   adj_open Float,
# MAGIC   adj_high Float,
# MAGIC   adj_low Float,
# MAGIC   adj_close Float,
# MAGIC   adj_volume Float)
# MAGIC USING com.databricks.spark.csv
# MAGIC OPTIONS (
# MAGIC   path "/DemoData/stock_data/WIKI_*.csv"
# MAGIC )

# COMMAND ----------

# MAGIC %sql select * from stock_data_csv

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Let's count the number of lines.  Note the amount of time it takes to parse the CSV file.

# COMMAND ----------

# MAGIC %sql select count(*) from stock_data_csv

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 3: Optimize by saving this table as parquet.

# COMMAND ----------

# MAGIC %sql DROP TABLE IF EXISTS stock_data

# COMMAND ----------

sqlContext.table("stock_data_csv").write.saveAsTable("stock_data")

# COMMAND ----------

# MAGIC %sql select * from stock_data

# COMMAND ----------

# MAGIC %sql select count(*) from stock_data