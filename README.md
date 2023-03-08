# Daily-Incremental-load-ETL-pipeline-for-Ecommerce-company-using-AWS-Lambda-and-Apache-airflow
Daily Incremental load ETL pipeline for Ecommerce company using AWS Lambda and AWS EMR cluster, Deployed using Apache airflow. 
Here, we are extracting Daily data from OLTP database(postgreSQL) using date and loading it into AWS S3 buckets. Triggering a insert job flow steps into EMR cluster using Lambda function for processing and Transformation and saving the processed data back to S3.
