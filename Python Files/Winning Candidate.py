# Databricks notebook source
# MAGIC %md
# MAGIC ##Create SQL table 👇

# COMMAND ----------

# MAGIC %fs rm -r 'dbfs:/user/hive/warehouse/'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP table If Exists Candidate;
# MAGIC DROP table If Exists Vote;
# MAGIC 
# MAGIC Create table If Not Exists Candidate (id int, Name STRING);
# MAGIC Create table If Not Exists Vote (id int, CandidateId int);
# MAGIC 
# MAGIC insert into Candidate (id, Name) values ('1', 'A');
# MAGIC insert into Candidate (id, Name) values ('2', 'B');
# MAGIC insert into Candidate (id, Name) values ('3', 'C');
# MAGIC insert into Candidate (id, Name) values ('4', 'D');
# MAGIC insert into Candidate (id, Name) values ('5', 'E');
# MAGIC 
# MAGIC insert into Vote (id, CandidateId) values ('1', '2');
# MAGIC insert into Vote (id, CandidateId) values ('2', '4');
# MAGIC insert into Vote (id, CandidateId) values ('3', '3');
# MAGIC insert into Vote (id, CandidateId) values ('4', '2');
# MAGIC insert into Vote (id, CandidateId) values ('5', '5');

# COMMAND ----------

# MAGIC %md
# MAGIC ###Converting SQL table to Dataframe 👇

# COMMAND ----------

Candidate_df=sqlContext.table("Candidate")
Vote_df=sqlContext.table("Vote")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Problem Statement: [⚠️ Note: Please Dont Run the Below Cell will through an error. It's Just for Question]

# COMMAND ----------

Table: Candidate
 +-----+---------+
 | id  | Name    |
 +-----+---------+
 | 1   | A       |
 | 2   | B       |
 | 3   | C       |
 | 4   | D       |
 | 5   | E       |
 +-----+---------+  
Table: Vote
 +-----+--------------+
 | id  | CandidateId  |
 +-----+--------------+
 | 1   |     2        |
 | 2   |     4        |
 | 3   |     3        |
 | 4   |     2        |
 | 5   |     5        |
 +-----+--------------+
it is the auto-increment primary key,
CandidateId is the id that appeared in the Candidate table.
Write a SQL to find the name of the winning candidate, the above example will return the winner B.
 +------+
 | Name |
 +------+
 | B    |
 +------+
Notes:
You may assume there is no tie, in other words there will be only one winning candidate.

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
# MAGIC select Name as Name
# MAGIC from Candidate
# MAGIC where id=
# MAGIC (select CandidateId
# MAGIC from Vote
# MAGIC group by CandidateId
# MAGIC order by count(id) desc
# MAGIC limit 1)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Solutions PySpark👇

# COMMAND ----------

from pyspark.sql.functions import col

# Get the Vote count of all Candidate
Vote_count_df=Vote_df.groupby(col('CandidateId')).count().orderBy(col('count').desc())
#Pull the row with top vote count
Winner_row=Vote_count_df.head(1)
#Get the candidate id by pulling the first row and first column
Winner_candidate=Winner_row[0][0]
#use the winner Candidate in filter
result_df=Candidate_df.select(col('Name')).filter(col('id')==Winner_candidate)

# COMMAND ----------

result_df.show()