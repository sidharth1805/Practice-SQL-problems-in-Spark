# Databricks notebook source
# MAGIC %md
# MAGIC ##Create SQL table 👇

# COMMAND ----------

# MAGIC %fs rm -r 'dbfs:/user/hive/warehouse/'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS  FriendRequest;
# MAGIC DROP TABLE IF EXISTS  RequestAccepted;
# MAGIC 
# MAGIC Create table If Not Exists FriendRequest (sender_id int, send_to_id int, request_date string);
# MAGIC Create table If Not Exists RequestAccepted (requester_id int, accepter_id int, accept_date string);
# MAGIC 
# MAGIC insert into FriendRequest (sender_id, send_to_id, request_date) values ('1', '2', '2016/06/01');
# MAGIC insert into FriendRequest (sender_id, send_to_id, request_date) values ('1', '3', '2016/06/01');
# MAGIC insert into FriendRequest (sender_id, send_to_id, request_date) values ('1', '4', '2016/06/01');
# MAGIC insert into FriendRequest (sender_id, send_to_id, request_date) values ('2', '3', '2016/06/02');
# MAGIC insert into FriendRequest (sender_id, send_to_id, request_date) values ('3', '4', '2016/06/09');
# MAGIC 
# MAGIC insert into RequestAccepted (requester_id, accepter_id, accept_date) values ('1', '2', '2016/06/03');
# MAGIC insert into RequestAccepted (requester_id, accepter_id, accept_date) values ('1', '3', '2016/06/08');
# MAGIC insert into RequestAccepted (requester_id, accepter_id, accept_date) values ('2', '3', '2016/06/08');
# MAGIC insert into RequestAccepted (requester_id, accepter_id, accept_date) values ('3', '4', '2016/06/09');
# MAGIC insert into RequestAccepted (requester_id, accepter_id, accept_date) values ('3', '4', '2016/06/10');

# COMMAND ----------

# MAGIC %md
# MAGIC ###Converting SQL table to Dataframe 👇

# COMMAND ----------

FriendRequest_df=sqlContext.table("FriendRequest")
RequestAccepted_df=sqlContext.table("RequestAccepted")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Problem Statement: [⚠️ Note: Please Dont Run the Below Cell will through an error. It's Just for Question]

# COMMAND ----------

Write an SQL query to find the overall acceptance rate of requests, which is the number of acceptance divided by the number of requests. Return the answer rounded to 2 decimals places.

Note that:

The accepted requests are not necessarily from the table friend_request. In this case, you just need to simply count the total accepted requests (no matter whether they are in the original requests), and divide it by the number of requests to get the acceptance rate.
It is possible that a sender sends multiple requests to the same receiver, and a request could be accepted more than once. In this case, the ‘duplicated’ requests or acceptances are only counted once.
If there are no requests at all, you should return 0.00 as the accept_rate.
The query result format is in the following example:

FriendRequest table:
+-----------+------------+--------------+
| sender_id | send_to_id | request_date |
+-----------+------------+--------------+
| 1         | 2          | 2016/06/01   |
| 1         | 3          | 2016/06/01   |
| 1         | 4          | 2016/06/01   |
| 2         | 3          | 2016/06/02   |
| 3         | 4          | 2016/06/09   |
+-----------+------------+--------------+

RequestAccepted table:
+--------------+-------------+-------------+
| requester_id | accepter_id | accept_date |
+--------------+-------------+-------------+
| 1            | 2           | 2016/06/03  |
| 1            | 3           | 2016/06/08  |
| 2            | 3           | 2016/06/08  |
| 3            | 4           | 2016/06/09  |
| 3            | 4           | 2016/06/10  |
+--------------+-------------+-------------+

Result table:
+-------------+
| unique_accepted_request |
+-------------+
| 4        |
+-------------+

+-------------+
| total_request |
+-------------+
| 5           |
+-------------+

 There are 4 unique accepted requests, and there are 5 requests in total. So the rate is 0.80.


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
# MAGIC SELECT COUNT (*) AS unique_request FROM  
# MAGIC (SELECT DISTINCT requester_id, accepter_id FROM REQUESTACCEPTED) A;
# MAGIC 
# MAGIC SELECT COUNT(*) AS total_request FROM  
# MAGIC (SELECT DISTINCT sender_id, send_to_id FROM FriendRequest) B;

# COMMAND ----------

# MAGIC %md
# MAGIC ###Solutions PySpark👇

# COMMAND ----------

a=RequestAccepted_df.select('requester_id','accepter_id').distinct().count()
b=FriendRequest_df.select('sender_id','send_to_id').distinct().count()

print(a/b)

# COMMAND ----------

a_df.show()

# COMMAND ----------

b_df.show()