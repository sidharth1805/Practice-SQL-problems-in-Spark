# Databricks notebook source
# MAGIC %md
# MAGIC ##Create SQL table 👇

# COMMAND ----------

# MAGIC %fs rm -r 'dbfs:/user/hive/warehouse/'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC Create table If Not Exists Calls (from_id int, to_id int, duration int);
# MAGIC 
# MAGIC insert into Calls (from_id, to_id, duration) values ('1', '2', '59');
# MAGIC insert into Calls (from_id, to_id, duration) values ('2', '1', '11');
# MAGIC insert into Calls (from_id, to_id, duration) values ('1', '3', '20');
# MAGIC insert into Calls (from_id, to_id, duration) values ('3', '4', '100');
# MAGIC insert into Calls (from_id, to_id, duration) values ('3', '4', '200');
# MAGIC insert into Calls (from_id, to_id, duration) values ('3', '4', '200');
# MAGIC insert into Calls (from_id, to_id, duration) values ('4', '3', '499');
# MAGIC insert into Calls (from_id, to_id, duration) values ('4', '1', '60');

# COMMAND ----------

# MAGIC %md
# MAGIC ###Converting SQL table to Dataframe 👇

# COMMAND ----------

calls_df=sqlContext.table("Calls")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Problem Statement: [⚠️ Note: Please Dont Run the Below Cell will through an error. It's Just for Question]

# COMMAND ----------

Table: Calls
+-------------+---------+
| Column Name | Type    |
+-------------+---------+
| from_id     | int     |
| to_id       | int     |
| duration    | int     |
+-------------+---------+
This table does not have a primary key, it may contain duplicates.
This table contains the duration of a phone call between from_id and to_id.
from_id != to_id

Write an SQL query to report the number of calls and 
the total call duration between each pair of distinct persons (person1, person2) 
where person1<person2.
Calls table:
+-------+-----+--------+
|from_id|to_id|duration|
+-------+-----+--------+
|      1|    2|      59|
|      2|    1|      11|
|      1|    3|      20|
|      3|    4|     100|
|      3|    4|     200|
|      3|    4|     200|
|      4|    3|     499|
|      4|    1|      60|
+-------+-----+--------+
Result table:
+-------+-------+----------+--------------+
|person1|person2|call_count|total_duration|
+-------+-------+----------+--------------+
|      1|      2|         2|            70|
|      1|      3|         1|            20|
|      3|      4|         4|           999|
|      1|      4|         1|            60|
+-------+-------+----------+--------------+
Users 1 and 2 had 2 calls and the total duration is 70 (59 + 11).
Users 1 and 3 had 1 call and the total duration is 20.
Users 1 and 4 had 1 call and the total duration is 60.
Users 3 and 4 had 4 calls and the total duration is 999 (100 + 200 + 200 + 499).


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
# MAGIC select
# MAGIC least(from_id,to_id) as person1,greatest(from_id,to_id) as person2,
# MAGIC count(duration) as call_count,sum(duration) as total_duration
# MAGIC from calls
# MAGIC group by person1,person2

# COMMAND ----------

# MAGIC %md
# MAGIC ###Solutions PySpark👇

# COMMAND ----------

from pyspark.sql.functions import least,greatest,count,sum

result_df = calls_df\
            .select(least(calls_df.from_id,calls_df.to_id).alias("person1"),\
             greatest(calls_df.from_id, calls_df.to_id).alias("person2"),'duration')\
            .groupby("person1","person2")\
            .agg(count("duration").alias("call_count"),sum("duration").alias("total_duration"))


# COMMAND ----------

calls_df.show()

# COMMAND ----------

result_df.show()