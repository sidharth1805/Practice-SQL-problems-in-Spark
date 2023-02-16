# Databricks notebook source
# MAGIC %md
# MAGIC ##Create SQL table 👇

# COMMAND ----------

# MAGIC %fs rm -r 'dbfs:/user/hive/warehouse/'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS  Friends;
# MAGIC DROP TABLE IF EXISTS  Activities;
# MAGIC 
# MAGIC Create table If Not Exists Friends (id int, name string, activity string);
# MAGIC Create table If Not Exists Activities (id int, name string);
# MAGIC 
# MAGIC insert into Friends (id, name, activity) values ('1', 'Jonathan D.', 'Eating');
# MAGIC insert into Friends (id, name, activity) values ('2', 'Jade W.', 'Singing');
# MAGIC insert into Friends (id, name, activity) values ('3', 'Victor J.', 'Singing');
# MAGIC insert into Friends (id, name, activity) values ('4', 'Elvis Q.', 'Eating');
# MAGIC insert into Friends (id, name, activity) values ('5', 'Daniel A.', 'Eating');
# MAGIC insert into Friends (id, name, activity) values ('6', 'Bob B.', 'Horse Riding');
# MAGIC 
# MAGIC insert into Activities (id, name) values ('1', 'Eating');
# MAGIC insert into Activities (id, name) values ('2', 'Singing');
# MAGIC insert into Activities (id, name) values ('3', 'Horse Riding');

# COMMAND ----------

# MAGIC %md
# MAGIC ###Converting SQL table to Dataframe 👇

# COMMAND ----------

friends_df=sqlContext.table("Friends")
Activities_df=sqlContext.table("Activities")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Problem Statement: [⚠️ Note: Please Dont Run the Below Cell will through an error. It's Just for Question]

# COMMAND ----------

Table: Friends
+---------------+---------+
| Column Name   | Type    |
+---------------+---------+
| id            | int     |
| name          | varchar |
| activity      | varchar |
+---------------+---------+
id is the id of the friend and primary key for this table.
name is the name of the friend.
activity is the name of the activity which the friend takes part in.
Table: Activities
+---------------+---------+
| Column Name   | Type    |
+---------------+---------+
| id            | int     |
| name          | varchar |
+---------------+---------+
id is the primary key for this table.
name is the name of the activity.

Write an SQL query to find the names of all the activities 
with neither maximum, nor minimum number of participants.
Return the result table in any order. 
Each activity in table Activities is performed by any person 
in the table Friends.
The query result format is in the following example:

Friends table:
+------+--------------+---------------+
| id   | name         | activity      |
+------+--------------+---------------+
| 1    | Jonathan D.  | Eating        |
| 2    | Jade W.      | Singing       |
| 3    | Victor J.    | Singing       |
| 4    | Elvis Q.     | Eating        |
| 5    | Daniel A.    | Eating        |
| 6    | Bob B.       | Horse Riding  |
+------+--------------+---------------+
Activities table:
+------+--------------+
| id   | name         |
+------+--------------+
| 1    | Eating       |
| 2    | Singing      |
| 3    | Horse Riding |
+------+--------------+
Result table:
+--------------+
| activity     |
+--------------+
| Singing      |
+--------------+

Eating activity is performed by 3 friends, maximum number of participants, (Jonathan D. , Elvis Q. and Daniel A.)
Horse Riding activity is performed by 1 friend, minimum number of participants, (Bob B.)
Singing is performed by 2 friends (Victor J. and Jade W.)


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
# MAGIC with CTE as (select activity a ,count(*) c
# MAGIC from friends f
# MAGIC group by activity)
# MAGIC 
# MAGIC select a as activity from CTE
# MAGIC where c != (select max(c) from CTE)
# MAGIC and c != (select min(c) from CTE)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Solutions PySpark👇

# COMMAND ----------

from pyspark.sql.functions import count, col
#CTE Dataframe
cte_df=friends_df.groupBy('activity').count()
#Pulling max and min
x=cte_df.agg({"count": "max"}).collect()[0][0]
y=cte_df.agg({"count": "min"}).collect()[0][0]
#Combing all
result_df=cte_df.filter((col('count')!=x) & (col('count')!=y))\
                .select('activity')

# COMMAND ----------

result_df.show()