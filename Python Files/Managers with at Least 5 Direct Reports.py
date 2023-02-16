# Databricks notebook source
# MAGIC %md
# MAGIC ##Create SQL table 👇

# COMMAND ----------

# MAGIC %fs rm -r 'dbfs:/user/hive/warehouse/'

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP table If Exists Employee;
# MAGIC Create table If Not Exists Employee (Id int, Name STRING, Department STRING, ManagerId int);
# MAGIC 
# MAGIC insert into Employee (Id, Name, Department, ManagerId) values ('101', 'John', 'A', Null);
# MAGIC insert into Employee (Id, Name, Department, ManagerId) values ('102', 'Dan', 'A', '101');
# MAGIC insert into Employee (Id, Name, Department, ManagerId) values ('103', 'James', 'A', '101');
# MAGIC insert into Employee (Id, Name, Department, ManagerId) values ('104', 'Amy', 'A', '101');
# MAGIC insert into Employee (Id, Name, Department, ManagerId) values ('105', 'Anne', 'A', '101');
# MAGIC insert into Employee (Id, Name, Department, ManagerId) values ('106', 'Ron', 'B', '101');

# COMMAND ----------

# MAGIC %md
# MAGIC ###Converting SQL table to Dataframe 👇

# COMMAND ----------

Employee_df=sqlContext.table("Employee")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Problem Statement: [⚠️ Note: Please Dont Run the Below Cell will through an error. It's Just for Question]

# COMMAND ----------

The Employee table holds all employees including their managers. Every employee has an Id, and there is also a column for the manager Id.

+------+----------+-----------+----------+
|Id    |Name      |Department |ManagerId |
+------+----------+-----------+----------+
|101   |John      |A          |null      |
|102   |Dan       |A          |101       |
|103   |James     |A          |101       |
|104   |Amy       |A          |101       |
|105   |Anne      |A          |101       |
|106   |Ron       |B          |101       |
+------+----------+-----------+----------+
Given the Employee table, write a SQL query that finds out managers with at least 5 direct report. For the above table, your SQL query should return:

+-------+
| Name  |
+-------+
| John  |
+-------+
Note:
No one would report to himself


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
# MAGIC SELECT Name 
# MAGIC FROM Employee 
# MAGIC WHERE id IN   
# MAGIC (SELECT ManagerId    
# MAGIC FROM Employee    
# MAGIC GROUP BY ManagerId    
# MAGIC HAVING COUNT(DISTINCT Id) >= 5)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Solutions PySpark👇

# COMMAND ----------

from pyspark.sql.functions import col,count_distinct

#Getting the required Manager ID's using groupby
ManagerId_df=Employee_df.groupby(col('ManagerId'))\
                                 .agg(count_distinct(col('Id')).alias("count"))\
                                 .filter("count>=5")\
                                 .select("ManagerId")

#Using the retrived Manager ID's to filter. 
#Using Left semi as in operator in SQL
result_df= Employee_df\
          .join(ManagerId_df, Employee_df.Id==ManagerId_df.ManagerId, 'leftsemi')\
          .select(Employee_df.Name)

# COMMAND ----------

result_df.show()