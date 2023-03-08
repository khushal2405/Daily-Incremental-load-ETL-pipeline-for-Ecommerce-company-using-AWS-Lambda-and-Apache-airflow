# Daily Incremental load ETL pipeline for Ecommerce company using AWS Lambda and Apache-airflow

![Pipeline Diagram]()


Daily Incremental load ETL pipeline for Ecommerce company using AWS Lambda and AWS EMR cluster, Deployed using Apache airflow. 
Here, we are extracting Daily data from OLTP database(postgreSQL) using date and loading it into AWS S3 buckets. Triggering a insert job flow steps into EMR cluster using Lambda function for processing and Transformation and saving the processed data to S3 where it will further triger the Lambda function.

## STEP 1 : Extracting Data from PostgreSQL and loading it into AWS s3 buckets !!!!
- for extraction I used Airflow Dags to extract and then load data into S3. We have to extract Daily Data from six SQL tables dated accoding to the day the row was inserted into the database (I have only considered orders and order-items table for daily incremental load)
- Airflow was deployed using docker along with all of it's other components(like redis and postgresdb for metadata).
- Because the incremental load is for ecommerce data I scheduled the Dag to run daily at 11 PM.

![Airflow DAG for dailt Incremental load](https://user-images.githubusercontent.com/40882021/223756657-ad69bb4c-b2f9-44b0-af81-bd93e0299d85.PNG)
### Note:
- Rather than using another postgres database as a dummy OLTP, I used airflow's postgres db that it uses to save metadata. It might save you some time
- We can also schedule a cron job instead of using airflow Dags to orchestrate these tasks of extraction of data daily.(Airflow is good for visualization)

## STEP 2 : Triggering of Lambda function
- When the orders file gets loaded into the S3 bucket, it triggers a Lambda function.
- This Lambda function insers the job to the idle running EMR cluster for processing and Transformation of data according to the requirement.

![AWS-labmda-s3-trigger](https://user-images.githubusercontent.com/40882021/223759417-8b28e18d-31a6-42bf-bafd-2738f5c48e73.PNG)
### Note
- the trigger was set such that after detecting any creation of object in the S3 bucket, it runs the lambda function written in python that submits the job to EMR cluster using it's command-runner.jar feature provided by AWS in EMR clusters.

### STEP 3 : 
- After completion of processing and transformation by running pyspark script in EMR, it writes the transformed data back to S3.
- This data is saved according to the extraction date so that it would be easier to pick the data for further loading into any data warehouse(like redshift, Hbase, Hive, etc). In this way we can make thie data available to dashboarding teams for their client serving analytics platforms.

## future scope
- rather than using EMR clusters, I can write a Glue job that gets triggered by lambda and then load it into Redshift data warehouse using copy command. 
- By using Glue I will be paying for only the resources that the glue scripts uses rather than provisioning lot of EMR clusters nodes saving the cost for unused ec2 instance resources.(Will try this in the next commit!!) 
