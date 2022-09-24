import json
import boto3
import requests
import urllib
import os
from datetime import datetime

def lambda_handler(event, context):
    region_name="ap-southeast-1"
    ec2_id="i-06e097119af25566b"
    username = os.environ.get('username')
    password = os.environ.get('password')
    ec2=boto3.client("ec2",region_name=region_name)
    response=ec2.describe_instances(InstanceIds=[ec2_id])
    public_dns=dns=response['Reservations'][0]['Instances'][0]['NetworkInterfaces'][0]['Association']['PublicDnsName']
    
    username = os.environ.get('username')
    password = os.environ.get('password')
    url="http://"+public_dns+":8080/api/v1/dags/Airflow_dags_7/dagRuns"
    print(f"url is {url}")
    Bucket = event['Records'][0]['s3']['bucket']['name']
    print(f"Bucket name is : {Bucket}")
    key_obj = event['Records'][0]['s3']['object']['key']
    key = urllib.parse.unquote_plus(key_obj, encoding='utf-8')
    print(f"Key is : {key}")
    dataset = key.split("/")[1]
    print(f"dataset is {dataset}")
    body = {"conf":{ "Bucket":Bucket, "key":key, "args":dataset}}
    headers = {'Content-Type': 'application/json'}
    r = requests.post(url,headers=headers,auth =(username,password), data=json.dumps(body))
    print({r})
    print(r.text)
    return r.status_code