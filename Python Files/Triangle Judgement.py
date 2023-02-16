# Databricks notebook source
# MAGIC %md
# MAGIC ##Create SQL table 👇

# COMMAND ----------

# MAGIC %fs rm -r 'dbfs:/user/hive/warehouse/'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP table If Exists triangle;
# MAGIC Create table If Not Exists triangle (x int, y int, z int);
# MAGIC 
# MAGIC insert into triangle (x, y, z) values ('13', '15', '30');
# MAGIC insert into triangle (x, y, z) values ('10', '20', '15');

# COMMAND ----------

# MAGIC %md
# MAGIC ###Converting SQL table to Dataframe 👇

# COMMAND ----------

triangle_df=sqlContext.table("triangle")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Problem Statement: [⚠️ Note: Please Dont Run the Below Cell will through an error. It's Just for Question]

# COMMAND ----------

A pupil Tim gets homework to identify 
whether three line segments could possibly form a triangle.
However, this assignment is very heavy 
because there are hundreds of records to calculate.
Could you help Tim by writing a query to judge whether these three  
sides can form a triangle, 
assuming table triangle holds the length of the three sides x, y and z.

| x  | y  | z  |
|----|----|----|
| 13 | 15 | 30 |
| 10 | 20 | 15 |

 For the sample data above, your query should return the follow result:
 | x  | y  | z  | triangle |
 |----|----|----|----------|
 | 13 | 15 | 30 | No       |
 | 10 | 20 | 15 | Yes      |

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
# MAGIC select x, y, z,    
# MAGIC case        
# MAGIC when (x + y) > z and (y + z) > x and (x + z) > y then 'Yes'        
# MAGIC else 'No' end as triangle from triangle;

# COMMAND ----------

# MAGIC %md
# MAGIC ###Solutions PySpark👇

# COMMAND ----------

from pyspark.sql.functions import col

#Importing when() function
from pyspark.sql.functions import when

#Use when otherwise function to replicate CASE WHEN ELSE 
result_df=triangle_df.withColumn('triangle',when((col('x')+col('y')>col('z')) &\
                                                 (col('z')+col('y')>col('x')) &\
                                                 (col('x')+col('z')>col('y')),"Yes")\
                                           .otherwise("No"))

# COMMAND ----------

result_df.show()