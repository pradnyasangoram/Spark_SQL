-- Databricks notebook source
select * from range(0,11)

-- COMMAND ----------

select * from employee1_csv

-- COMMAND ----------

select sum(JoiningYear) from employee1_csv

-- COMMAND ----------

 show tables

-- COMMAND ----------

select * from employee1_csv

-- COMMAND ----------

-- filet : where
select * from employee1_csv where Education == "Masters" and Age > 30

-- COMMAND ----------

-- sorting age (older to younger) order by

select * from employee1_csv order by Age asc

-- COMMAND ----------

-- aggregate: sum, mean, avg, min, max. 

select avg(Age), min(Age), max(age) from employee1_csv

-- COMMAND ----------

-- groupig 
select  max(age), City from employee1_csv 
group by  City

-- COMMAND ----------

-- joining: inner, outer, left,right


-- COMMAND ----------

-- union

-- COMMAND ----------

-- CTE : common table expression is like a named query, whose result can be stored in a virtual table(a CTE) to be reference in the main query
with virtual_table as(
  select * from employee1_csv
) 
select * from virtual_table


-- COMMAND ----------

show tables

-- COMMAND ----------

select * from customer_payment_csv

-- COMMAND ----------

-- window function : row_numbber, rank and dense_rank
select *, row_number() over(partition by Education order by Education) as rn from employee1_csv

-- COMMAND ----------

-- remove duplicate recorders 

with cte as (
select *, row_number() over(partition by Education order by Education) as rn from employee1_csv
)
select * from cte where rn=1


-- COMMAND ----------

-- view : create a temp view/table

create or replace temp view remove_duplicates as 
select *, row_number() over(partition by Education order by Education) as rn from employee1_csv

-- COMMAND ----------

select * from remove_duplicates

-- COMMAND ----------


