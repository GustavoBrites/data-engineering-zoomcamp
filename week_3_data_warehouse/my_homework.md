## Homework
[Form](https://forms.gle/ytzVYUh2RptgkvF79)  
We will use all the knowledge learned in this week. Please answer your questions via form above.  
**Deadline** for the homework is 14th Feb 2022 17:00 CET.

### Question 1: 
**What is count for fhv vehicles data for year 2019**  
Can load the data for cloud storage and run a count(*)


SELECT COUNT (*) 
FROM `mystical-nimbus-338219.trips_data_all.fhv_tripdata_external_table` 

### Question 2: 
**How many distinct dispatching_base_num we have in fhv for 2019**  
Can run a distinct query on the table from question 1

SELECT COUNT (DISTINCT dispatching_base_num) 
FROM `mystical-nimbus-338219.trips_data_all.fhv_tripdata_external_table` 

### Question 3: 
**Best strategy to optimise if query always filter by dropoff_datetime and order by dispatching_base_num**  
Review partitioning and clustering video.   
We need to think what will be the most optimal strategy to improve query 
performance and reduce cost.

### Question 4: 
**What is the count, estimated and actual data processed for query which counts trip between 2019/01/01 and 2019/03/31 for dispatching_base_num B00987, B02060, B02279**  
Create a table with optimized clustering and partitioning, and run a 
count(*). Estimated data processed can be found in top right corner and
actual data processed can be found after the query is executed.

used the DAG in gcs_to_bq_dag_hw3q4.py to create the partitioned_clustered table
used the date '2019-03-30' to give the answer from the available options. 


SELECT COUNT(*) FROM `mystical-nimbus-338219.trips_data_all.fhv_tripdata_partitioned_clustered2` 
WHERE DATE(dropoff_datetime)  BETWEEN '2019-01-01' AND '2019-03-30' 
AND dispatching_base_num IN ('B00987','B02060','B02279')


### Question 5: 
**What will be the best partitioning or clustering strategy when filtering on dispatching_base_num and SR_Flag**  
Review partitioning and clustering video. 
Clustering cannot be created on all data types.

### Question 6: 
**What improvements can be seen by partitioning and clustering for data size less than 1 GB**  
Partitioning and clustering also creates extra metadata.  
Before query execution this metadata needs to be processed.

### (Not required) Question 7: 
**In which format does BigQuery save data**  
Review big query internals video.
