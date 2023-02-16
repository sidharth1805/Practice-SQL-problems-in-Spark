# Databricks notebook source
# MAGIC %md
# MAGIC ##Create SQL table 👇

# COMMAND ----------

# MAGIC %fs rm -r 'dbfs:/user/hive/warehouse/'

# COMMAND ----------

# MAGIC %sql
# MAGIC Create table If Not Exists ActorDirector (actor_id int, director_id int, timestamp int);
# MAGIC insert into ActorDirector (actor_id, director_id, timestamp) values ('1', '1', '0');
# MAGIC insert into ActorDirector (actor_id, director_id, timestamp) values ('1', '1', '1');
# MAGIC insert into ActorDirector (actor_id, director_id, timestamp) values ('1', '1', '2');
# MAGIC insert into ActorDirector (actor_id, director_id, timestamp) values ('1', '2', '3');
# MAGIC insert into ActorDirector (actor_id, director_id, timestamp) values ('1', '2', '4');
# MAGIC insert into ActorDirector (actor_id, director_id, timestamp) values ('2', '1', '5');
# MAGIC insert into ActorDirector (actor_id, director_id, timestamp) values ('2', '1', '6');

# COMMAND ----------

# MAGIC %md
# MAGIC ###Converting SQL table to Dataframe 👇

# COMMAND ----------

ActorDirector_df=sqlContext.table("ActorDirector")

# COMMAND ----------

ActorDirector_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Problem Statement: [⚠️ Note: Please Dont Run the Below Cell will through an error. It's Just for Question]

# COMMAND ----------

+-------------+---------+
| Column Name | Type    |
+-------------+---------+
| actor_id    | int     |
| director_id | int     |
| timestamp   | int     |
+-------------+---------+

Timestamp is the primary key column for this table.

Write a SQL query for a report that provides the pairs (actor_id, director_id) 
where the actor have co-worked with the director at least 3 times.

Example:

ActorDirector table:
+-------------+-------------+-------------+
| actor_id    | director_id | timestamp   |
+-------------+-------------+-------------+
| 1           | 1           | 0           |
| 1           | 1           | 1           |
| 1           | 1           | 2           |
| 1           | 2           | 3           |
| 1           | 2           | 4           |
| 2           | 1           | 5           |
| 2           | 1           | 6           |
+-------------+-------------+-------------+

Result table:
+-------------+-------------+
| actor_id    | director_id |
+-------------+-------------+
| 1           | 1           |
+-------------+-------------+

The only pair is (1, 1) where they co-worked exactly 3 times.


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
# MAGIC select actor_id,director_id
# MAGIC from ActorDirector
# MAGIC group by actor_id,director_id
# MAGIC having count(actor_id)>2

# COMMAND ----------

# MAGIC %md
# MAGIC ###Solutions PySpark👇

# COMMAND ----------

from pyspark.sql.functions import count

ans=ActorDirector_df.groupBy('actor_id','director_id')\
                   .agg(count('actor_id')>2)\
                   .where(count('actor_id')>2)\
                   .select('actor_id','director_id')

# COMMAND ----------

ans.show()