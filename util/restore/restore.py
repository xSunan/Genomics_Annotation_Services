# restore.py
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
config.read('restore_config.ini')

'''Capstone - Exercise 9
Restore results files for users who upgrade to premium
'''
def handle_restore_queue(sqs=None):

  # Read a message from the queue
  print(f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())} - Polling queue {config['sqs']['RestoreQueueName']} . . . .")

  # Must use long polling, i.e. WaitTimeSeconds = max allowed
  try:
    messages = sqs.receive_message(
      QueueUrl=config['sqs']['RestoreQueueUrl'],
      WaitTimeSeconds=20,
      MaxNumberOfMessages=10)
  except ClientError as e:
    print(f"Failed to read message(s) from restore queue: {e}")
    return

  if 'Messages' in messages:
    for message in messages['Messages']:
      # Extract the message body and transform it to a dictionary
      restore_data = json.loads(json.loads(message['Body'])['Message'])

      # Extract job parameters from the message body
      job_id = restore_data['job_id']

      # Initiate a retrieval task in Glacier
      glacier = boto3.client('glacier', region_name=config['aws']['AwsRegionName'])

      # We store the GAS job ID in the description field so we can get
      # other required details when restoring the archive to S3
      try:
        # Attempt an expedited retrieval
        response = glacier.initiate_job(
          vaultName=config['glacier']['Vault'],
          jobParameters={
            'Type': 'archive-retrieval',
            'ArchiveId': restore_data['archive_id'],
            'Description': job_id,
            'SNSTopic': config['sns']['ResultsThawTopic'],
            'Tier': config['glacier']['PreferredRetrievalTier']})
        restore_status = 'RESTORING_EXPEDITED'

      except glacier.exceptions.InsufficientCapacityException as e:
        # No expedited capacity available; degrade to standard retrieval
        try:
          response = glacier.initiate_job(
            vaultName=config['glacier']['Vault'],
            jobParameters={
              'Type': 'archive-retrieval',
              'ArchiveId': restore_data['archive_id'],
              'Description': job_id,
              'SNSTopic': config['sns']['ResultsThawTopic'],
              'Tier': config['glacier']['DefaultRetrievalTier']})
          restore_status = 'RESTORING_STANDARD'

        except ClientError as e:
          print(f"Standard retrieval request failed with error: {e}")
          return

      except ClientError as e:
        print(f"Expedited retrieval request failed with error: {e}")
        return

      # Update the archive job ID key in DynamoDB so that
      # we can tell the user when the archival is in progress
      db = boto3.resource('dynamodb', region_name=config['aws']['AwsRegionName'])
      ann_table = db.Table(config['dynamodb']['AnnotationsTable'])
      try:

        ann_item = ann_table.update_item(
          Key={'job_id': job_id},
          UpdateExpression="SET results_file_archive_id = :rfai",
          ExpressionAttributeValues={':rfai': restore_status})
      except ClientError as e:
        print(f"Failed to update job with restore status: {e}")
        return

      print(f"Requested restore of results file for job ID {job_id}")

      # Delete ARCHIVE task message from queue
      try:
        sqs.delete_message(
          QueueUrl=config['sqs']['RestoreQueueUrl'],
          ReceiptHandle=message['ReceiptHandle'])
      except ClientError as e:
        print(f"Failed to delete restore message for job {job_id}: {e}")
        return

      print(f"Deleted restore request message with ID {message['MessageId']}")


if __name__ == '__main__':  
  # Use SQS Client in order to set message visibility
  sqs = boto3.client('sqs', region_name=config['aws']['AwsRegionName'])

  # Check if archive queue exists, otherwise create it
  try:
    sqs.get_queue_url(QueueName=config['sqs']['RestoreQueueName'])
  except sqs.exceptions.QueueDoesNotExist as err:
    print(f"Queue {config['sqs']['RestoreQueueName']} not found; creating it...")
    try:
      sqs.create_queue(QueueName=config['sqs']['RestoreQueueName'])
    except ClientError as e:
      print(f'Failed to create restore queue: {e}')
      sys.exit()

  # Poll queue for new results and process them
  while True:
    handle_restore_queue(sqs=sqs)
    time.sleep(10)   # taking a little nap since restoration time can vary

### EOF