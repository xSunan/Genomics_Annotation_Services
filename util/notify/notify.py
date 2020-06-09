# notify.py
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


# import SQS

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
from configparser import SafeConfigParser
config = SafeConfigParser(os.environ)
config.read('notify_config.ini')

# Add utility code here
def poll_result_queue():
    # Connect to SQS and get the message queue, and connect to s3
    try:
        sqs = boto3.client('sqs', region_name=config['aws']['AwsRegionName'])
        queue_url = config['aws']['SQS_RESULT_URL']
    except boto3.exceptions.ResourceNotExistsError as e:
        print(e)

    while True:
        # long polling the messages with wait time seconds set to 20s
        try:
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Client.receive_message
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
            user_id,receipt_handle, job_id = extract_info(message)
        
            # retrieve user related info
            profile = helpers.get_user_profile(id = user_id, db_name =config['postgre']['DB_NAME'])
            print(profile)
            print(type(profile))
            user_email = profile['email']
            user_name = profile['name']
            print(user_id+" "+job_id+" "+user_name+" "+user_email)

            # construct email related info
            sender = config['aws']['EMAIL_SENDER']
            recipients = user_email
            subject = "Job Finished"
            detail_url = "https://xsunan.ucmpcs.org/annotations/{}".format(job_id)
            body = "Dear {},\nYour submitted job {} has completed.\
             You can check via the below website.\n{}".format(user_name, job_id,detail_url)

            # send the email
            helpers.send_email_ses(recipients=recipients, sender = sender, subject=subject, body = body)

            # delete the message
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
    except (json.JSONDecodeError, KeyError):
        print("Error: Input is not valid json format or doesn't have corresponding key")
        return None

    print("succ return")
    return user_id,receipt_handle, job_id 

if __name__ == '__main__':
    poll_result_queue()
### EOF