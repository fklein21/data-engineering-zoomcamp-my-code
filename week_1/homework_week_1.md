# Homework 1

## Question 1: Google Cloud SDK

Output of `gcloud --version`
```
Google Cloud SDK 370.0.0
alpha 2022.01.21
beta 2022.01.21
bq 2.0.73
core 2022.01.21
gsutil 5.6
minikube 1.24.0
skaffold 1.35.1
```

## Question 2: Terraform

Output of the `terraform` commands:

```
 ~/data-engineering-zoomcamp/1_terraform_gcp/terraform  terraform init

Initializing the backend...

Initializing provider plugins...
- Reusing previous version of hashicorp/google from the dependency lock file
- Using previously-installed hashicorp/google v4.8.0

Terraform has been successfully initialized!

You may now begin working with Terraform. Try running "terraform plan" to see
any changes that are required for your infrastructure. All Terraform commands
should now work.

If you ever set or change modules or backend configuration for Terraform,
rerun this command to reinitialize your working directory. If you forget, other
commands will detect it and remind you to do so if necessary.


 ~/data-engineering-zoomcamp/1/terraform  terraform plan

Terraform used the selected providers to generate the following execution plan. Resource actions are indicated with the following symbols:
  + create

Terraform will perform the following actions:

  # google_bigquery_dataset.dataset will be created
  + resource "google_bigquery_dataset" "dataset" {
      + creation_time              = (known after apply)
      + dataset_id                 = "trips_data_all"
      + delete_contents_on_destroy = false
      + etag                       = (known after apply)
      + id                         = (known after apply)
      + last_modified_time         = (known after apply)
      + location                   = "europe-west6"
      + project                    = "dtc-de-339301"
      + self_link                  = (known after apply)

      + access {
          + domain         = (known after apply)
          + group_by_email = (known after apply)
          + role           = (known after apply)
          + special_group  = (known after apply)
          + user_by_email  = (known after apply)

          + view {
              + dataset_id = (known after apply)
              + project_id = (known after apply)
              + table_id   = (known after apply)
            }
        }
    }

  # google_storage_bucket.data-lake-bucket will be created
  + resource "google_storage_bucket" "data-lake-bucket" {
      + force_destroy               = true
      + id                          = (known after apply)
      + location                    = "EUROPE-WEST6"
      + name                        = "dtc_data_lake_dtc-de-339301"
      + project                     = (known after apply)
      + self_link                   = (known after apply)
      + storage_class               = "STANDARD"
      + uniform_bucket_level_access = true
      + url                         = (known after apply)

      + lifecycle_rule {
          + action {
              + type = "Delete"
            }

          + condition {
              + age                   = 30
              + matches_storage_class = []
              + with_state            = (known after apply)
            }
        }

      + versioning {
          + enabled = true
        }
    }

Plan: 2 to add, 0 to change, 0 to destroy.

─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────

Note: You didn't use the -out option to save this plan, so Terraform can't guarantee to take exactly these actions if you run "terraform apply" now.


 ~/data-engineering-zoomcamp/1/terraform  terraform apply

Terraform used the selected providers to generate the following execution plan. Resource actions are indicated with the following symbols:
  + create

Terraform will perform the following actions:

  # google_bigquery_dataset.dataset will be created
  + resource "google_bigquery_dataset" "dataset" {
      + creation_time              = (known after apply)
      + dataset_id                 = "trips_data_all"
      + delete_contents_on_destroy = false
      + etag                       = (known after apply)
      + id                         = (known after apply)
      + last_modified_time         = (known after apply)
      + location                   = "europe-west6"
      + project                    = "dtc-de-339301"
      + self_link                  = (known after apply)

      + access {
          + domain         = (known after apply)
          + group_by_email = (known after apply)
          + role           = (known after apply)
          + special_group  = (known after apply)
          + user_by_email  = (known after apply)

          + view {
              + dataset_id = (known after apply)
              + project_id = (known after apply)
              + table_id   = (known after apply)
            }
        }
    }

  # google_storage_bucket.data-lake-bucket will be created
  + resource "google_storage_bucket" "data-lake-bucket" {
      + force_destroy               = true
      + id                          = (known after apply)
      + location                    = "EUROPE-WEST6"
      + name                        = "dtc_data_lake_dtc-de-339301"
      + project                     = (known after apply)
      + self_link                   = (known after apply)
      + storage_class               = "STANDARD"
      + uniform_bucket_level_access = true
      + url                         = (known after apply)

      + lifecycle_rule {
          + action {
              + type = "Delete"
            }

          + condition {
              + age                   = 30
              + matches_storage_class = []
              + with_state            = (known after apply)
            }
        }

      + versioning {
          + enabled = true
        }
    }

Plan: 2 to add, 0 to change, 0 to destroy.

Do you want to perform these actions?
  Terraform will perform the actions described above.
  Only 'yes' will be accepted to approve.

  Enter a value: yes

google_bigquery_dataset.dataset: Creating...
google_storage_bucket.data-lake-bucket: Creating...
google_bigquery_dataset.dataset: Creation complete after 2s [id=projects/dtc-de-339301/datasets/trips_data_all]
google_storage_bucket.data-lake-bucket: Creation complete after 3s [id=dtc_data_lake_dtc-de-339301]

Apply complete! Resources: 2 added, 0 changed, 0 destroyed.

```

## Question 3: Count records

How many taxi trips were there on January 15?

Consider only trips that started on January 15.

The code: 
```
SELECT 
	COUNT(1)
FROM 
	yellow_taxi_data
WHERE
	date_trunc('DAY', tpep_pickup_datetime) = DATE('2021-01-15');
```

> Result: 53024

## Question 4: Largest tip for each day
Find the largest tip for each day.

The code:
```
SELECT 
	CAST(tpep_pickup_datetime AS DATE) AS "day",
	MAX(tip_amount) AS "max_tip"
FROM 
	yellow_taxi_data
GROUP BY
	"day"
ORDER BY
	"day" ASC;
```

The result:
```
"2008-12-31"	0
"2009-01-01"	0
"2020-12-31"	4.08
"2021-01-01"	158
"2021-01-02"	109.15
"2021-01-03"	369.4
"2021-01-04"	696.48
"2021-01-05"	151
"2021-01-06"	100
"2021-01-07"	95
"2021-01-08"	100
"2021-01-09"	230
"2021-01-10"	91
"2021-01-11"	145
"2021-01-12"	192.61
"2021-01-13"	100
"2021-01-14"	95
"2021-01-15"	99
"2021-01-16"	100
"2021-01-17"	65
"2021-01-18"	90
"2021-01-19"	200.8
"2021-01-20"	1140.44
"2021-01-21"	166
"2021-01-22"	92.55
"2021-01-23"	100
"2021-01-24"	122
"2021-01-25"	100.16
"2021-01-26"	250
"2021-01-27"	100
"2021-01-28"	77.14
"2021-01-29"	75
"2021-01-30"	199.12
"2021-01-31"	108.5
"2021-02-01"	1.54
"2021-02-22"	1.76
```

On which day it was the largest tip in January?

The code: 
```
SELECT 
	CAST("tpep_pickup_datetime" AS DATE) AS "day",
	MAX(tip_amount) AS "max_tip"
FROM 
	yellow_taxi_data
WHERE
	DATE(tpep_pickup_datetime) BETWEEN '2021-01-01' AND '2021-01-31'
GROUP BY
	"day"
ORDER BY
	"max_tip" DESC
LIMIT 1;

```

The result: 
```
"2021-01-20"	1140.44
```

## Question 5. Most popular destination

What was the most popular destination for passengers picked up in central park on January 14?

The code: 
```
SELECT 
	z2."Zone" AS "dropOffLoc",
	COUNT(1) AS "count"
FROM 
	yellow_taxi_data t,
	zones z1,
	zones z2
WHERE
	t."PULocationID" = z1."LocationID" AND
	z1."Zone" = 'Central Park'
	AND
	t."DOLocationID" = z2."LocationID" 
	AND
	DATE(t."tpep_pickup_datetime") = '2021-01-14'
GROUP BY
	1
ORDER BY
	"count" DESC
LIMIT 1;
```

The result:
```
"Upper East Side South"	97
```

## Question 6: Most expensive locations
What's the pickup-dropoff pair with the largest average price for a ride (calculated based on total_amount)?

The code: 
```
SELECT 
	CONCAT(z1."Zone", ' / ', z2."Zone") AS "pickUpDropOffLoc",
	CONCAT(z1."LocationID", ' / ', z2."LocationID") AS "LocId",
	AVG(total_amount) AS "avg_price"
FROM 
	yellow_taxi_data t,
	zones z1,
	zones z2
WHERE
	t."PULocationID" = z1."LocationID"
	AND
	t."DOLocationID" = z2."LocationID"
GROUP BY
	1, 2
ORDER BY
	3 DESC
LIMIT 1;
```

The result: 
```
"Alphabet City / "	"4 / 265"	2292.4
```

