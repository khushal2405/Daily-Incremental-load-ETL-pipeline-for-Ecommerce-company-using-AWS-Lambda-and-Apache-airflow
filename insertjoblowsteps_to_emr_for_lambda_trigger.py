import json
import boto3
import logging
from botocore.exceptions import ClientError
import os


def lambda_handler(event, context):
    #these cluster id and sparks steps can alo be loaded into a config file or json file and then loaded using agparser
    # or can be taken as arguments of python script while submitting the job into cluster using command-runner.jar in EMR clsuter
    jobflowid = cluster_id = "j-2xxCUxxxxNXxX"

    steps = SPARK_STEPS = [
        {
            "Name": "incremental-load-lambda-project",
            "ActionOnFailure": "CANCEL_AND_WAIT",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--master", "yarn",
                    "--executor-memory", "2g",
                    "--executor-cores", "3",
                    "--deploy-mode",
                    "cluster",
                    "s3://s3-lambda-to-s3-project/emr_script/EMR-script-for-lambda-project.py",

                ],
            },
        }
    ]

    # Upload the file
    emr_client = boto3.client('emr', region_name='ap-south-1', aws_access_key_id='xxxxxxxxxxxx',
                              aws_secret_access_key='xxxxxxxxxxxxxxxxxxxxxxxx')

    try:
        response = emr_client.add_job_flow_steps(JobFlowId=jobflowid, Steps=steps)
    except ClientError as e:
        logging.error(e)
        return False
    return True


