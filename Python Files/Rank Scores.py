# Databricks notebook source
# MAGIC %md
# MAGIC ##Create SQL table 👇

# COMMAND ----------

# MAGIC %fs rm -r 'dbfs:/user/hive/warehouse/'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS  Scores;
# MAGIC 
# MAGIC Create table If Not Exists Scores (Id int, Score DECIMAL(3,2));
# MAGIC 
# MAGIC insert into Scores (Id, Score) values ('1', '3.5');
# MAGIC insert into Scores (Id, Score) values ('2', '3.65');
# MAGIC insert into Scores (Id, Score) values ('3', '4.0');
# MAGIC insert into Scores (Id, Score) values ('4', '3.85');
# MAGIC insert into Scores (Id, Score) values ('5', '4.0');
# MAGIC insert into Scores (Id, Score) values ('6', '3.65');

# COMMAND ----------

# MAGIC %md
# MAGIC ###Converting SQL table to Dataframe 👇

# COMMAND ----------

Scores_df=sqlContext.table("Scores")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Problem Statement: [⚠️ Note: Please Dont Run the Below Cell will through an error. It's Just for Question]

# COMMAND ----------

Write a SQL query to rank scores. If there is a tie between two scores, both should have the same ranking. 
Note that after a tie, the next ranking number should be the next consecutive integer value. 
In other words, there should be no "holes" between ranks.

+----+-------+
| Id | Score |
+----+-------+
| 1  | 3.50  |
| 2  | 3.65  |
| 3  | 4.00  |
| 4  | 3.85  |
| 5  | 4.00  |
| 6  | 3.65  |
+----+-------+
For example, given the above Scores table, your query should generate the following report (order by highest score):

+-------+---------+
| score | Rank    |
+-------+---------+
| 4.00  | 1       |
| 4.00  | 1       |
| 3.85  | 2       |
| 3.65  | 3       |
| 3.65  | 3       |
| 3.50  | 4       |
+-------+---------+

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
# MAGIC select 
# MAGIC Score,
# MAGIC dense_rank() over (order by Score desc) as RANK
# MAGIC from Scores

# COMMAND ----------

# MAGIC %md
# MAGIC ###Solutions PySpark👇

# COMMAND ----------

#Importing the window and Dense_Rank Functions
from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank

#Defining the Window Spec
windowSpec  = Window.orderBy(Scores_df.Score.desc())

#Applying the Window Spec to the Data Frame
result_df=Scores_df.withColumn("Rank",dense_rank().over(windowSpec))

result_df.show()

# COMMAND ----------

result_df.show()