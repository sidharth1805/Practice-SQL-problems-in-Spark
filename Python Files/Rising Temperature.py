# Databricks notebook source
# MAGIC %md
# MAGIC ##Create SQL table 👇

# COMMAND ----------

# MAGIC %fs rm -r 'dbfs:/user/hive/warehouse/'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS  Weather;
# MAGIC 
# MAGIC Create table If Not Exists Weather (Id int, RecordDate DATE, Temperature int);
# MAGIC 
# MAGIC insert into Weather (Id, RecordDate, Temperature) values ('1', '2015-01-01', '10');
# MAGIC insert into Weather (Id, RecordDate, Temperature) values ('2', '2015-01-02', '25');
# MAGIC insert into Weather (Id, RecordDate, Temperature) values ('3', '2015-01-03', '20');
# MAGIC insert into Weather (Id, RecordDate, Temperature) values ('4', '2015-01-04', '30');

# COMMAND ----------

# MAGIC %md
# MAGIC ###Converting SQL table to Dataframe 👇

# COMMAND ----------

Weather_df=sqlContext.table("Weather")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Problem Statement: [⚠️ Note: Please Dont Run the Below Cell will through an error. It's Just for Question]

# COMMAND ----------

Table: Weather
+---------------+---------+
| Column Name   | Type    |
+---------------+---------+
| id            | int     |
| recordDate    | date    |
| temperature   | int     |
+---------------+---------+
id is the primary key for this table.
This table contains information about the temperature in a certain day.

Write an SQL query to find all dates' 
id with higher temperature compared to its previous dates (yesterday).

Return the result table in any order.
The query result format is in the following example:

Weather
+----+------------+-------------+
| id | recordDate | Temperature |
+----+------------+-------------+
| 1  | 2015-01-01 | 10          |
| 2  | 2015-01-02 | 25          |
| 3  | 2015-01-03 | 20          |
| 4  | 2015-01-04 | 30          |
+----+------------+-------------+

Result table:
+----+
| Id |
+----+
| 2  |
| 4  |
+----+

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ###Provide Your SQL Solution in below Cell 👇

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ###Provide Your Spark ✨ Solution in below Cell 👇

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ###Solutions 👇

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select w1.Id as Id
# MAGIC from weather w1 inner join weather w2
# MAGIC on w1.recordDate=w2.recordDate+1
# MAGIC where w1.temperature>w2.temperature

# COMMAND ----------

# MAGIC %md
# MAGIC ###Solutions PySpark👇

# COMMAND ----------

from pyspark.sql.functions import col

result_df=   Weather_df.alias("today")\
                    .join(Weather_df.alias("yesterday"),col('today.RecordDate')==col('yesterday.RecordDate')+1,'inner')\
                    .filter(col('today.Temperature')>col('yesterday.Temperature'))\
                    .select(col('today.Id'))

# COMMAND ----------

result_df.show()