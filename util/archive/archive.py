# archive.py
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

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
from configparser import SafeConfigParser
config = SafeConfigParser(os.environ)
config.read('archive_config.ini')

# Add utility code here
def relocate_result():
    # Connect to SQS and get the archive message queue, and connect to s3
    try:
        sqs = boto3.client('sqs', region_name=config['aws']['AwsRegionName'])
        queue_url = config['aws']['SQS_ARCHIVE_URL']
    except boto3.exceptions.ResourceNotExistsError as e:
        print(e)

    while True:
        # long polling the messages with wait time seconds set to 20s
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Client.receive_message
        try:
            result_response = sqs.receive_message(
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
            messages = result_response['Messages']
            if len(messages) ==0:
                continue
        except KeyError:
            continue

        for message in messages:            
            user_id,receipt_handle, job_id,s3_key_annot_file  = extract_info(message)

            # retrieve user related info
            profile = helpers.get_user_profile(id = user_id, db_name =config['postgre']['DB_NAME'])

            # check if the user is still a free user
            if profile['role'] == "premium_user":
                delete_message(sqs, queue_url, receipt_handle)
                continue

            # retreive the result file
            # Create a session client to the S3 service
            s3 = boto3.client('s3', 
                region_name=config['aws']['AwsRegionName'])
            bucket_name = config['aws']['AWS_S3_RESULTS_BUCKET']

            # get the log file's object and read it
            try:
                # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.get_object
                result_file = s3.get_object(Bucket= bucket_name,Key=s3_key_annot_file)
                content = result_file['Body'].read()
            except botocore.exceptions.ClientError as e:
                print(e)

            # relocate the result file from s3 to glacier
            try: 
                # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier.html#Glacier.Client.upload_archive
                glacier = boto3.client('glacier',region_name=config['aws']['AwsRegionName'])
                vault = config['aws']['VAULT_NAME']
                response = glacier.upload_archive(vaultName=vault,body=content)
                archive_id = response['ResponseMetadata']['HTTPHeaders']['x-amz-archive-id']
            except (glacier.exceptions.ResourceNotFoundException, glacier.exceptions.InvalidParameterValueException,\
            glacier.exceptions.MissingParameterValueException,glacier.exceptions.RequestTimeoutException,\
            glacier.exceptions.ServiceUnavailableException) as e:
                print(e)

            # update the status in dynamodb (add archive_id attribute and set the result_file existence attribute)
            # connect to the dynamoDB
            try:
                dynamodb = boto3.resource('dynamodb', region_name=config['aws']['AwsRegionName'])
                table_name = config['aws']['DYNAMODB_TABLE_NAME']
                ann_table = dynamodb.Table(table_name)
            except (ClientError, boto3.exceptions.ResourceNotExistsError) as e:
                print(e)
            
            try:
                # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.update_item
                response = ann_table.update_item(
                    Key={
                        'job_id': job_id
                    },
                    UpdateExpression="set results_file_archive_id =:r, existed = :e",
                    ExpressionAttributeValues={
                        ':r': archive_id,
                        ':e': "False"
                    },
                )        
            except (ClientError) as e:
                print(e.response['Error']['Message'])
                continue   

            # delete the result file in s3
            try:
                # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.delete_object
                s3.delete_object(Bucket = bucket_name,Key = s3_key_annot_file)
            except ClientError as e:
                print(e) 

            # delete the message
            delete_message(sqs, queue_url,receipt_handle)
        

def delete_message(sqs, queue_url,receipt_handle):
    '''delete the message '''
    try:
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Client.delete_message
        dele_rsp = sqs.delete_message(
            QueueUrl = queue_url,
            ReceiptHandle = receipt_handle
        )
    except (boto3.exceptions.ClientError,SQS.Client.exceptions.InvalidIdFormat, SQS.Client.exceptions.ReceiptHandleIsInvalid) as e:
        print(e)


def extract_info(message):
    try:
        content = json.loads(message['Body'])
        receipt_handle = message['ReceiptHandle']
        data = json.loads(content['Message'])
        job_id = data['job_id']
        user_id = data['user_id']
        s3_key_annot_file = data['s3_key_annot_file']
    except (json.JSONDecodeError, KeyError):
        print("Error: Input is not valid json format or doesn't have corresponding key")
        return None

    return user_id,receipt_handle, job_id, s3_key_annot_file

if __name__ == '__main__':
    relocate_result()

### EOF