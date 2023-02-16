# Databricks notebook source
# MAGIC %md
# MAGIC ##Create SQL table 👇

# COMMAND ----------

# MAGIC %fs rm -r 'dbfs:/user/hive/warehouse/'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE If Not Exists point (x INT NOT NULL);
# MAGIC 
# MAGIC insert into point (x) values ('-1');
# MAGIC insert into point (x) values ('0');
# MAGIC insert into point (x) values ('2');

# COMMAND ----------

# MAGIC %md
# MAGIC ###Converting SQL table to Dataframe 👇

# COMMAND ----------

point_df=sqlContext.table("point")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Problem Statement: [⚠️ Note: Please Dont Run the Below Cell will through an error. It's Just for Question]

# COMMAND ----------

Table point holds the x coordinate of some points on x-axis in a plane, 
which are all integers.
Write a query to find the shortest distance between two points in these points.
| x   |
|-----|
| -1  |
| 0   |
| 2   |

The shortest distance is '1' obviously, which is from point '-1' to '0'.
So the output is as below:
| shortest|
|---------|
|  1      |


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
# MAGIC select abs(a1.x-b1.x) as shortest 
# MAGIC from point a1
# MAGIC cross join point b1 
# MAGIC where a1.x!=b1.x
# MAGIC order by shortest
# MAGIC limit 1

# COMMAND ----------

# MAGIC %md
# MAGIC ###Solutions PySpark👇

# COMMAND ----------

from pyspark.sql.functions import abs, col

result_df=  point_df.alias("a1").crossJoin(point_df.alias("b1"))\
                    .where(col("a1.x") != col("b1.x"))\
                    .select(abs(col("a1.x") - col("b1.x")).alias("shortest"))\
                    .orderBy("shortest")\
                    .limit(1)


# COMMAND ----------

point_df.show()

# COMMAND ----------

result_df.show()