# Databricks notebook source
# MAGIC %md
# MAGIC ##Create SQL table 👇

# COMMAND ----------

# MAGIC %fs rm -r 'dbfs:/user/hive/warehouse/'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC Create table If Not Exists Employee (Id int, Name varchar(255), Salary int, DepartmentId int);
# MAGIC Create table If Not Exists Department (Id int, Name varchar(255));
# MAGIC 
# MAGIC insert into Employee (Id, Name, Salary, DepartmentId) values ('1', 'Joe', '70000', '1');
# MAGIC insert into Employee (Id, Name, Salary, DepartmentId) values ('2', 'Jim', '90000', '1');
# MAGIC insert into Employee (Id, Name, Salary, DepartmentId) values ('3', 'Henry', '80000', '2');
# MAGIC insert into Employee (Id, Name, Salary, DepartmentId) values ('4', 'Sam', '60000', '2');
# MAGIC insert into Employee (Id, Name, Salary, DepartmentId) values ('5', 'Max', '90000', '1');
# MAGIC 
# MAGIC insert into Department (Id, Name) values ('1', 'IT');
# MAGIC insert into Department (Id, Name) values ('2', 'Sales');

# COMMAND ----------

# MAGIC %md
# MAGIC ###Converting SQL table to Dataframe 👇

# COMMAND ----------

employee_df=sqlContext.table("Employee")
department_df=sqlContext.table("Department")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Problem Statement: [⚠️ Note: Please Dont Run the Below Cell will through an error. It's Just for Question]

# COMMAND ----------

The Employee table holds all employees. 
Every employee has an Id, a salary, and there is also a column for the department Id.
+----+-------+--------+--------------+
| Id | Name  | Salary | DepartmentId |
+----+-------+--------+--------------+
| 1  | Joe   | 70000  | 1            |
| 2  | Jim   | 90000  | 1            |
| 3  | Henry | 80000  | 2            |
| 4  | Sam   | 60000  | 2            |
| 5  | Max   | 90000  | 1            |
+----+-------+--------+--------------+

The Department table holds all departments of the company.
+----+----------+
| Id | Name     |
+----+----------+
| 1  | IT       |
| 2  | Sales    |
+----+----------+

Write a SQL query to find employees who have the highest salary in each of the departments.
    
+------------+----------+--------+
| Department | Employee | Salary |
+------------+----------+--------+
| IT         | Max      | 90000  |
| IT         | Jim      | 90000  |
| Sales      | Henry    | 80000  |
+------------+----------+--------+

Explanation:
Max and Jim both have the highest salary in the IT department and Henry has the highest salary in the Sales department.


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
# MAGIC select d.name as Department,e.name as Employee,e.Salary as Salary
# MAGIC from Employee e inner join Department d
# MAGIC on e.DepartmentId=d.Id
# MAGIC where (e.DepartmentId ,e.Salary) IN
# MAGIC (SELECT e.DepartmentId, MAX(e.Salary) 
# MAGIC FROM Employee e
# MAGIC GROUP BY e.DepartmentId)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Solutions PySpark👇

# COMMAND ----------

from pyspark.sql.functions import max

c1=employee_df.groupBy(employee_df.DepartmentId)\
              .agg(max(employee_df.Salary).alias('Max_Salary'))

c2=employee_df.join(department_df,employee_df.DepartmentId==department_df.Id)

#Coverting the max salary department wise into a list.Reason: to be able to use in isin() operator
c1_Salary_list     = c1.select('Max_Salary').rdd.flatMap(lambda x: x).collect()
c1_Department_list = c1.select('DepartmentId').rdd.flatMap(lambda x: x).collect()

from pyspark.sql.functions import col

c3=c2.filter((col('Salary').isin(c1_Salary_list)) & (col('DepartmentId').isin(c1_Department_list)))\
     .select(department_df.Name,employee_df.Name,employee_df.Salary)

# COMMAND ----------

c3.show()