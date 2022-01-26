/* ONe way of doing JOINS - INNERJOIN*/

SELECT 
tpep_pickup_datetime,
tpep_dropoff_datetime,
total_amount,
CONCAT(zpu."Borough" , ' / ', zpu."Zone") AS "pick_up_loc",
CONCAT(zpu."Borough" , ' / ' ,zpu."Zone") AS "dropoff_loc"
FROM    
 yellow_taxi_trips t, /* t is the alias */
 zones zpu,
 zones zdo
WHERE
t."PULocationID" = zpu."LocationID" AND
t."DULocationID" = zdo."LocationID"
/* in quotes due to capital start*/ 
LIMIT 100; /* first 100 lines */

/* or */

FROM
yellow t JOIN zones zpu 
ON /* previous WHERE */ 
t."PULocationID" = zpu."LocationID"
JOIN  zones zdo
t."DULocationID" = zdo."LocationID"
LIMIT 100;
/* */
SELECT 
tpep_pickup_datetime
FROM 
yellow_taxi_trips t
WHERE 
tpep_pickup_datetime is NULL
LIMIT 100;

SELECT 
"DOLocationID"
FROM 
yellow_taxi_trips t
WHERE 
"DOLocationID" NOT IN (SELECT "LocationID" FROM zones)
LIMIT 100;

DELETE FROM zones WHERE "LocationID" = 142;

/* LEFT JOIN */

FROM
yellow t LEFT JOIN zones zpu 
ON /* previous WHERE */ 
t."PULocationID" = zpu."LocationID"
LESS JOIN  zones zdo
t."DULocationID" = zdo."LocationID"
LIMIT 100;

/* GROUP BY */
SELECT /* columns of table */
/*
DATE_TRUNC('DAY',tpep_dropoff_datetime), 
removes the time and leaves only the day
OR */
CAST(tpep_dropoff_datetime AS DATE) as "day", /* novo nome da coluna */
"DOLocationID",
COUNT(1) as"count",/* counts the records */
MAX(total_amount),
MAX(passenger_count)
FROM 
    yellow_taxi_trips
GROUP BY 
    CAST(tpep_dropoff_datetime AS DATE)
    /* 1 2 - first 2 columns */
ORDER BY "day" ASC,
"DULocationID" DESC;

/* see the day with largest number of records */
ORDER BY "count" DESC;



