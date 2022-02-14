  -- Homework for week 3

  -- Question 1:
  -- What is count for fhv vehicles data for year 2019
  -- Can load the data for cloud storage and run a count(*)
  -- Using the external table:
SELECT
  COUNT(*)
FROM
  `dtc-de-339301.trips_data_all.fhv_tripdata_external_table`;

  -- ... and now the partitioned table:
SELECT
  COUNT(*)
FROM
  `dtc-de-339301.trips_data_all.fhv_tripdata_part`;
  -- Result: 42084899


  -- Question 2:
  -- How many distinct dispatching_base_num we have in fhv for 2019
  -- Can run a distinct query on the table from question 1
SELECT
  COUNT(DISTINCT dispatching_base_num)
FROM
  `dtc-de-339301.trips_data_all.fhv_tripdata_part`
WHERE
  DATE(pickup_datetime) < '2020-01-01'
  AND DATE(pickup_datetime) > '2018-12-31';
  -- Result: 792


  -- Question 3:
  -- Best strategy to optimise if query always filter by dropoff_datetime and order by dispatching_base_num
  -- We need to think what will be the most optimal strategy to improve query performance and reduce cost.
  -- For an optimal strategy:
  -- - Partition by dropoff_datetime because the query has to search only in the partitions that are selected by our filter.
  -- - Cluster by dispatching_base_num because clustering maintains the order of the rows in each partition. So the data in each partition is already in order which keeps the ORDER BY operation time-efficient.


  -- Question 4:
  -- What is the count, estimated and actual data processed for query which counts trip between 2019/01/01 and 2019/03/31 for dispatching_base_num B00987, B02060, B02279
SELECT
  COUNT(*)
FROM
  `dtc-de-339301.trips_data_all.fhv_tripdata_external_table`
WHERE
  DATE(pickup_datetime) BETWEEN '2019-01-01'
  AND '2019-04-01'
  AND dispatching_base_num IN ('B00987',
    'B02060',
    'B02279');
  -- Job info: (8.7 sec elapsed, 429.2 MB processed), estimated: 0B

  -- Now the partitioned and clustered table
CREATE OR REPLACE TABLE
  `dtc-de-339301.trips_data_all.fhv_tripdata_part_clust`
PARTITION BY
  DATE(pickup_datetime)
CLUSTER BY
  dispatching_base_num AS
SELECT
  *
FROM
  `dtc-de-339301.trips_data_all.fhv_tripdata_external_table`;

SELECT
  COUNT(*)
FROM
  `dtc-de-339301.trips_data_all.fhv_tripdata_part_clust`
WHERE
  DATE(pickup_datetime) BETWEEN '2019-01-01'
  AND '2019-04-01'
  AND dispatching_base_num IN ('B00987',
    'B02060',
    'B02279');
  -- Job info: (0.9 sec elapsed, 145.7 MB processed), estimated: 401.1MB


  -- Question 5:
  -- What will be the best partitioning or clustering strategy when filtering on dispatching_base_num and SR_Flag
  -- Review partitioning and clustering video. Clustering cannot be created on all data types.
SELECT
  COUNT(DISTINCT dispatching_base_num)
FROM
  `dtc-de-339301.trips_data_all.fhv_tripdata_part`
WHERE
  DATE(pickup_datetime) < '2020-01-01'
  AND DATE(pickup_datetime) > '2018-12-31';
  -- Result: 792
SELECT
  COUNT(DISTINCT SR_Flag)
FROM
  `dtc-de-339301.trips_data_all.fhv_tripdata_part`
WHERE
  DATE(pickup_datetime) < '2020-01-01'
  AND DATE(pickup_datetime) > '2018-12-31';
  -- Result: 43

  -- Partitioning cannot be performed on the dispatching_base_num column, since it contains strings.
  -- One could perform partitioning on the SR_Flag column and the cluster the dispatching_base_num column.
  -- Other things also have to be considered: Will the data in all partitions be modified frequently?
  --          Does it result in partitions < 1GB. ...

  
  -- Question 6:
  -- What improvements can be seen by partitioning and clustering for data size less than 1 GB
  -- Partitioning and clustering also creates extra metadata.
  -- Before query execution this metadata needs to be processed.
  -- If the partitions are too small the time needed for processing the meta data
  -- outweighs the time saved by partitioning the data.