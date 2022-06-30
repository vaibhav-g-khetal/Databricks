-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Assignment 2

-- COMMAND ----------

-- 1. Create a Spark database demo_db

create database if not exists demo_db

-- COMMAND ----------

-- 2. Create a spark table fire_service_calls_tbl in the demo_db

create table if not exists demo_db.fire_service_calls_tbl(
  CallNumber integer,
  UnitID string,
  IncidentNumber integer,
  CallType string,
  CallDate string,
  WatchDate string,
  CallFinalDisposition string,
  AvailableDtTm string,
  Address string,
  City string,
  Zipcode integer,
  Battalion string,
  StationArea string,
  Box string,
  OriginalPriority string,
  Priority string,
  FinalPriority integer,
  ALSUnit boolean,
  CallTypeGroup string,
  NumAlarms integer,
  UnitType string,
  UnitSequenceInCallDispatch integer,
  FirePreventionDistrict string,
  SupervisorDistrict string,
  Neighborhood string,
  Location string,
  RowID string,
  Delay float
) using parquet

-- COMMAND ----------

-- 3. Load data into fire_service_calls from the fire_service_calls_view
insert into demo_db.fire_service_calls_tbl 
select * from global_temp.fire_service_calls_view

-- COMMAND ----------

select * from demo_db.fire_service_calls_tbl 

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Assignment 3

-- COMMAND ----------

-- 1. How many distinct types of calls were made to the fire department?

select count(distinct callType) from demo_db.fire_service_calls_tbl where calltype is not null 

-- COMMAND ----------

-- 2. What are distinct types of calls made to the fire department?

select distinct(callType) from demo_db.fire_service_calls_tbl where calltype is not null 

-- COMMAND ----------

-- 3. Find out all responses or delayed times greater than 5 mins?

select CallNumber, Delay from demo_db.fire_service_calls_tbl where delay > 5

-- COMMAND ----------

-- 4. What were the most common call types?

select CallType, count(1) as Count from demo_db.fire_service_calls_tbl where calltype is not null group by CallType order by Count DESC

-- COMMAND ----------

-- 5. What zip codes accounted for the most common calls?

select CallType, Zipcode, count(1) as Count from demo_db.fire_service_calls_tbl where calltype is not null group by Zipcode, CallType order by Count desc

-- COMMAND ----------

-- 6. What San Francisco neighborhoods are in the zip codes 94102 and 94103

select Neighborhood, Zipcode from demo_db.fire_service_calls_tbl where Zipcode== 94102 or Zipcode== 94103

-- COMMAND ----------

-- 7. What was the sum of all calls, average, min, and max of the call response times?

select sum(NumAlarms), avg(delay), min(delay), max(delay) from demo_db.fire_service_calls_tbl 

-- COMMAND ----------

-- 8. How many distinct years of data are in the CSV file?

-- select distinct year(to_date(CallDate, "MM/dd/yyyy")) as year_num from demo_db.fire_service_calls_tbl order by year_num

select count(distinct year(to_date(CallDate, "MM/dd/yyyy"))) as count from demo_db.fire_service_calls_tbl 

-- COMMAND ----------

-- 9. What week of the year in 2018 had the most fire calls?

select weekofyear(to_date(CallDate, "MM/dd/yyyy")) week_year, count(1) as count from demo_db.fire_service_calls_tbl 
where year(to_date(CallDate, "MM/dd/yyyy")) == 2018
group by week_year order by count desc

-- COMMAND ----------

-- 10. What neighborhoods in San Francisco had the worst response time in 2018?

select Neighborhood , delay from demo_db.fire_service_calls_tbl 
where year(to_date(CallDate,"MM/dd/yyyy")) == 2018 order by delay desc

-- COMMAND ----------


