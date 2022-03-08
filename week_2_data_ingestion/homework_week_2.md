# Week 2 Homework

### Ferdinand Kleinschroth

The code for:
* [docker-compose.yaml](airflow/docker-compose.yaml)
* [Dockerfile](airflow/Dockerfile)
* [helper_fun.py](airflow/dags/helper_fun.py) ( functionsfor parquetizing and uploading)

## Question 1: Start date for the Yellow taxi data (1 point)

You'll need to parametrize the DAG for processing the yellow taxi data that
we created in the videos. 

The start data is
* 2019-01-01

The code can be found here: [yellow_data_ingestion_to_gcp_dag.py](airflow/dags/yellow_data_ingestion_to_gcp_dag.py)


## Question 2: Frequency for the Yellow taxi data (1 point)

How often do we need to run this DAG?

* Monthly


## Question 3: DAG for FHV Data (2 points)

Question: how many DAG runs are green for data in 2019 after finishing everything? 

* There are 12 green DAG runs, one for each month.

The code can be found here: [fhv_data_ingestion-to_gcp_dag.py](airflow/dags/fhv_data_ingestion-to_gcp_dag.py)


## Question 4: DAG for Zones (2 points)

How often does it need to run?

* Once (since the zones don't change each month)

The code can be found here: 
[zones_data_ingestion-to_gcp_dag.py](airflow/dags/zones_data_ingestion-to_gcp_dag.py)

