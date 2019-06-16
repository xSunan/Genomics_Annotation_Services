# notify.py
#
# NOTE: This file lives on the Utils instance
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import boto3
import time
import os
import sys
import json
import psycopg2
from botocore.exceptions import ClientError

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
from configparser import SafeConfigParser
config = SafeConfigParser(os.environ)
config.read('notify_config.ini')

'''Capstone - Exercise 3(e)

Reads result messages from SQS and sends notification emails.
'''
def handle_results_queue(sqs=None):
  # Read a message from the queue
  print(f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())} - Polling queue {config['sqs']['ResultsQueueName']} . . . .")

  # Must use long polling, i.e. WaitTimeSeconds = max allowed
  try:
    messages = sqs.receive_message(
      QueueUrl=config['sqs']['ResultsQueueUrl'],
      WaitTimeSeconds=20,
      MaxNumberOfMessages=10)
  except ClientError as e:
    print(f"Failed to read message(s) from results queue: {e}")
    return

  if 'Messages' in messages:
    for message in messages['Messages']:
      # Extract the message body and transform it to a dictionary
      results_data = json.loads(json.loads(message['Body'])['Message'])

      # Extract job parameters from the message body
      job_id = results_data['job_id']

      # Get user ID from jobs database
      db = boto3.resource('dynamodb', region_name=config['aws']['AwsRegionName'])
      jobs_table = db.Table(config['dynamodb']['AnnotationsTable'])
      try:
        job = jobs_table.get_item(Key={'job_id': job_id})
        if 'Item' in job:
          job = job['Item']
        else:
          # Item not found in database
          print(f'Job {job_id} not found in database')
          return 
      except ClientError as e:
        return
      user_id = job['user_id']

      # Get email address from accounts database
      try:
        profile = helpers.get_user_profile(id=user_id)
      except ClientError as e:
        return
      except psycopg2.Error as e:
        return

      user_email = profile['email']
      complete_time = results_data['complete_time']
      print(f"Received results for job ID {job_id}")

      # Send notification email via SES
      body = f'''
      Annotation job {job_id} completed at {complete_time}.\n\n\
      Click here to view results: {config['gas']['AnnotationsUrl'] + job_id}
      '''
      try:
        response = helpers.send_email_ses(recipients=user_email, 
          subject="GAS results available",
          body=body)
      except ClientError as e:
        print(f"Failed to send notification email: {e}")

      if (response['ResponseMetadata']['HTTPStatusCode'] == 200):
        # Delete message from queue
        try:
          sqs.delete_message(
            QueueUrl=config['sqs']['ResultsQueueUrl'],
            ReceiptHandle=message['ReceiptHandle'])
        except ClientError as e:
          print(f"Failed to delete message from results queue: {e}")

        print(f"Deleted results message with ID {message['MessageId']}")


if __name__ == '__main__':
  # Connect to SQS 
  sqs = boto3.client('sqs', region_name=config['aws']['AwsRegionName'])
  
  # Check if results queue exists, otherwise create it
  try:
    sqs.get_queue_url(QueueName=config['sqs']['ResultsQueueName'])
  except sqs.exceptions.QueueDoesNotExist as err:
    print(f"Queue {config['sqs']['ResultsQueueName']} not found; creating it...")
    try:
      sqs.create_queue(QueueName=config['sqs']['ResultsQueueName'])
    except ClientError as e:
      print(f'Failed to create job results queue: {e}')
      sys.exit()

  # Poll queue for new results and process them
  while True:
    handle_results_queue(sqs=sqs)

### EOF