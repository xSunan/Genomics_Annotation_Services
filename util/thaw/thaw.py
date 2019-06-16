# thaw.py
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
from botocore.exceptions import ClientError

# Get configuration
from configparser import SafeConfigParser
config = SafeConfigParser(os.environ)
config.read('thaw_config.ini')

'''Capstone - Exercise 9
Restore thawed results files back to S3
'''
def handle_thaw_queue(sqs=None):
  # Read a message from the queue
  print(f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())} - Polling queue {config['sqs']['ThawQueueName']} . . . .")

  # Must use long polling, i.e. WaitTimeSeconds = max allowed
  try:
    messages = sqs.receive_message(
      QueueUrl=config['sqs']['ThawQueueUrl'],
      WaitTimeSeconds=20,
      MaxNumberOfMessages=10)
  except ClientError as e:
    print(f"Failed to read message(s) from thaw queue: {e}")
    return

  if 'Messages' in messages:
    for message in messages['Messages']:
      # Extract the message body and transform it to a dictionary
      thaw_data = json.loads(json.loads(message['Body'])['Message'])

      # Get the GAS job ID; it was stashed in the description 
      # field when we archived the results file
      job_id = str(thaw_data['JobDescription'])
      
      # Get the GAS job from DynamoDB
      db = boto3.resource('dynamodb', region_name=config['aws']['AwsRegionName'])
      ann_table = db.Table(config['dynamodb']['AnnotationsTable'])
      try:
        ann_item = ann_table.get_item(Key={'job_id': job_id})['Item']
      except ClientError as e:
        print(f"Failed to get thawing job from the database: {e}")
        return

      # Read the thawed bits from Glacier
      glacier = boto3.client('glacier', region_name=config['aws']['AwsRegionName'])
      try:
        thawed_object = glacier.get_job_output(
          vaultName=config['glacier']['Vault'],
          jobId=thaw_data['JobId'])
      except ClientError as e:
        print(f"Unable to get output for thaw job {thaw_data['JobId']}: {e}")
        return

      # Store thawed bits in S3 results bucket
      s3 = boto3.client('s3', region_name=config['aws']['AwsRegionName'])
      try:
        s3.put_object(
          Bucket=config['s3']['ResultsBucket'],
          Key=ann_item['s3_key_result_file'],
          Body=thawed_object['body'].read())
      except ClientError as e:
        print(f"Unable to read thawed object: {e}")
        return

      # Delete Glacier archive
      try:
        glacier.delete_archive(
          vaultName=config['glacier']['Vault'],
          archiveId=thaw_data['ArchiveId'])
      except ClientError as e:
        print(f"Unable to delete Glacier archive {thaw_data['ArchiveId']}: {e}")

      # Remove Glacier archive ID key from DynamoDB item
      try:
        ann_item = ann_table.update_item(
          Key={'job_id': job_id},
          UpdateExpression="REMOVE results_file_archive_id")
      except ClientError as e:
        print(f"Unable to remove archive attribute for job {job_id}: {e}")

      print(f"Restored results file for job {job_id}")

      # Delete message from queue
      try:
        sqs.delete_message(
          QueueUrl=config['sqs']['ThawQueueUrl'],
          ReceiptHandle=message['ReceiptHandle'])
      except ClientError as e:
        print(f"Failed to delete thaw message for job {job_id}: {e}")
        return
      
      print(f"Deleted thaw message with ID {message['MessageId']}")


if __name__ == '__main__':  
  sqs = boto3.client('sqs', region_name=config['aws']['AwsRegionName'])

  # Check if thaw queue exists, otherwise create it
  try:
    sqs.get_queue_url(QueueName=config['sqs']['ThawQueueName'])
  except sqs.exceptions.QueueDoesNotExist as err:
    print(f"Queue {config['sqs']['ThawQueueName']} not found; creating it...")
    try:
      sqs.create_queue(QueueName=config['sqs']['ThawQueueName'])
    except ClientError as e:
      print(f'Failed to create thaw queue: {e}')
      sys.exit()

  # Poll queue for new results and process them
  while True:
    handle_thaw_queue(sqs=sqs)
    time.sleep(30)   # taking a longer nap since thawing time is longer/indeterminate

### EOF