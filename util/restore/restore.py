# restore.py
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

import boto3
import botocore
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
from configparser import SafeConfigParser
config = SafeConfigParser(os.environ)
config.read('restore_config.ini')

# Add utility code here
def initiate_restore():
    try:
        sqs = boto3.client('sqs', region_name = config['aws']['AwsRegionName'])
        queue_url = config['aws']['SQS_RESTORE_URL']
    except boto3.exceptions.ResourceNotExistsError as e:
        print(e)

    while True:
        # long polling the messages with wait time seconds set to 20s
        try:
            restore_response = sqs.receive_message(
                QueueUrl=queue_url,
                AttributeNames=[
                    'SentTimestamp'
                ],
                MaxNumberOfMessages=5,
                WaitTimeSeconds=20
            )
        except botocore.errorfactory.QueueDoesNotExist as e:
            print(e)
            continue

        try:
            messages = restore_response['Messages']
            if len(messages) ==0:
                continue
        except KeyError:
            continue
        # print(len(messages))
        for message in messages:
            # print(message)
            content = json.loads(message['Body'])
            receipt_handle = message['ReceiptHandle']
            data = json.loads(content['Message'])

            user_id = data['user_id'] 

            archives = get_user_archive(user_id)
            # connect to glacier
            glacier = boto3.client('glacier', region_name=config['aws']['AwsRegionName'])
            # initiate restore job
            for archive in archives:
                description = json.dumps({'job_id':archive['job_id'],'s3_key':archive['s3_key_result_file']}) 
                try:   
                    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier.html#Glacier.Client.initiate_job
                    response = glacier.initiate_job(
                        vaultName = config['aws']['VAULT_NAME'],
                        jobParameters = {
                            'Type': "archive-retrieval",
                            'ArchiveId': archive['archive_id'],
                            'SNSTopic': config['aws']['SNS_THAW_TOPIC'],
                            'Tier': 'Expedited',
                            'Description': description
                        }
                    )
                # botocore.errorfactory.InsufficientCapacityException 
                except:
                    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier.html#Glacier.Client.initiate_job
                    response = glacier.initiate_job(
                        vaultName = config['aws']['VAULT_NAME'],
                        jobParameters = {
                            'Type': "archive-retrieval",
                            'ArchiveId': archive['archive_id'],
                            'SNSTopic': config['aws']['SNS_THAW_TOPIC'],
                            'Tier': 'Standard',
                            'Description': description
                        }
                    )
                # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier.html#Glacier.Client.describe_job
                status = glacier.describe_job(vaultName=config['aws']['VAULT_NAME'],
                    jobId=response['jobId'])

            delete_message(sqs,queue_url,receipt_handle)
            


def delete_message(sqs,queue_url,receipt_handle):
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Client.delete_message
    try:
        dele_rsp = sqs.delete_message(
            QueueUrl = queue_url,
            ReceiptHandle = receipt_handle
        )
    except (boto3.exceptions.ClientError,SQS.Client.exceptions.InvalidIdFormat, SQS.Client.exceptions.ReceiptHandleIsInvalid) as e:
        print(e)

def get_user_archive(user_id):
    try:
        dynamodb = boto3.resource('dynamodb', region_name=config['aws']['AwsRegionName'])
        table_name = config['aws']['AWS_DYNAMODB_ANNOTATIONS_TABLE']
        ann_table = dynamodb.Table(table_name)
    except (ClientError, boto3.exceptions.ResourceNotExistsError) as e:
        print(e)
        return None

    try:
        # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/dynamodb.html#querying-and-scanning
        response = ann_table.query(
            IndexName = 'user_id_index',
            KeyConditionExpression=Key('user_id').eq(user_id)
        )
    except ClientError:
        print(e)
        return None

    items = response['Items']
    archives = []
    for job in items:
        archive = {
            "s3_key_result_file": job['s3_key_result_file'],
            "archive_id": job['results_file_archive_id'],
            "job_id": job['job_id']
        }
        archives.append(archive)

    return archives

if __name__ == '__main__':
    initiate_restore()




### EOF