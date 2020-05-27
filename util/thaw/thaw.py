# thaw.py
#
# NOTE: This file lives on the Utils instance
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import os
import sys
import json
from pprint import pprint

import threading

import boto3
import botocore
from botocore.exceptions import ClientError

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
from configparser import SafeConfigParser
config = SafeConfigParser(os.environ)
config.read('thaw_config.ini')

# Add utility code here
def monitor_job():
    try:
        sqs = boto3.client('sqs', region_name = config['aws']['AwsRegionName'])
        queue_url = config['aws']['SQS_THAW_URL']
    except boto3.exceptions.ResourceNotExistsError as e:
        print(e)

    while True:
        # long polling the messages with wait time seconds set to 20s
        try:
            thaw_response = sqs.receive_message(
                QueueUrl=queue_url,
                AttributeNames=[
                    'SentTimestamp'
                ],
                MaxNumberOfMessages=5,
                WaitTimeSeconds=20
            )
        except botocore.errorfactory.QueueDoesNotExist as e:
            print(e)
            continue;

        
        try:
            messages = thaw_response['Messages']
            if len(messages) ==0:
                continue
        except KeyError:
            continue

        print(len(messages))

        for message in messages:
            print(message)
            content = json.loads(message['Body'])
            receipt_handle = message['ReceiptHandle']
            # delete_message(sqs,queue_url,receipt_handle)
            try:
                data = json.loads(content['Message'])
                # pprint(data)
                restore_job_id = data['JobId']
                info = json.loads(data['JobDescription'])
                s3_result_key = info['s3_key']
                job_id = info['job_id']
            except :
                # job_id = content['Message']
                continue

            print(s3_result_key)

            # connect to glacier
            glacier = boto3.client('glacier', region_name=config['aws']['AwsRegionName'])
            # handle_result = threading.Thread(target = handle_result_file, args = (glacier, job_id,s3_result_key, ))
            # handle_result.start()
            job_resp = glacier.get_job_output(vaultName=config['aws']['VAULT_NAME'],
                    jobId=restore_job_id)
            file_content = job_resp['body']
            print("startupload: "+s3_result_key)

            #upload to the s3
            s3 = boto3.client('s3',region_name=config['aws']['AwsRegionName'])
            s3.upload_fileobj(file_content, config['aws']['AWS_S3_RESULTS_BUCKET'], s3_result_key)

            # update the result_file exist status in dynamoDB
            # connect to the dynamoDB
            try:
                dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
                table_name = config['aws']['DYNAMODB_TABLE_NAME']
                ann_table = dynamodb.Table(table_name)
            except (ClientError, boto3.exceptions.ResourceNotExistsError) as e:
                print(e)
            
            try:
                response = ann_table.update_item(
                    Key={
                        'job_id': job_id
                    },
                    UpdateExpression="set existed = :e",
                    ExpressionAttributeValues={
                        ':e': "True"
                    },
                )        
            except (ClientError) as e:
                print(e.response['Error']['Message'])
                continue   

            delete_message(sqs,queue_url,receipt_handle)
            
            
            
def delete_message(sqs,queue_url,receipt_handle):
    try:
        dele_rsp = sqs.delete_message(
            QueueUrl = queue_url,
            ReceiptHandle = receipt_handle
        )
    except (boto3.exceptions.ClientError,SQS.Client.exceptions.InvalidIdFormat, SQS.Client.exceptions.ReceiptHandleIsInvalid) as e:
        print(e)


            
# def handle_result_file(glacier,job_id,s3_result_key):
#     while True:
#         status = glacier.describe_job(vaultName=config['aws']['VAULT_NAME'],
#                         jobId=job_id)
#         if status['Completed']:
#             break
#         time.sleep(300)
    
#     job_resp = glacier.get_job_output(vaultName=config['aws']['VAULT_NAME'],
#             jobId=job_id)
#     file_content = job_resp['body']
#     print("startupload: "+s3_result_key)
#     #upload to the s3
#     s3 = boto3.client('s3',region_name=config['aws']['AwsRegionName'])
#     s3.upload_fileobj(file_content, config['aws']['AWS_S3_RESULTS_BUCKET'], s3_result_key)


if __name__=='__main__':
    monitor_job()
### EOF