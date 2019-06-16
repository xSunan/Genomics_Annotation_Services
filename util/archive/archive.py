# archive.py
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
config.read('archive_config.ini')

'''Capstone - Exercise 7
Archive free user results files
'''
def handle_archive_queue(sqs=None):
  # Read a message from the queue
  print(f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())} - Polling queue {config['sqs']['ArchiveQueueName']} . . . .")

  # Must use long polling, i.e. WaitTimeSeconds = max allowed
  try:
    messages = sqs.receive_message(
      QueueUrl=config['sqs']['ArchiveQueueUrl'],
      WaitTimeSeconds=20,
      MaxNumberOfMessages=10)
  except ClientError as e:
    print(f"Failed to read message(s) from archive queue: {e}")
    return

  if 'Messages' in messages:
    for message in messages['Messages']:
      # Extract the message body and transform it to a dictionary
      archive_data = json.loads(json.loads(message['Body'])['Message'])

      # Extract job parameters from the message body
      job_id = archive_data['job_id']

      # Check if it's time to archive this job's results
      archive_after = int(archive_data['archive_after'])
      time_to_archive = archive_after - int(time.time())

      if (time_to_archive <= 0):
        db = boto3.resource('dynamodb', region_name=config['aws']['AwsRegionName'])
        ann_table = db.Table(config['dynamodb']['AnnotationsTable'])
        try:
          ann_item = ann_table.get_item(Key={'job_id': job_id})['Item']
        except ClientError as e:
          print(f"Failed to get job to be archived from the database: {e}")
          return

        # Archive the jobs results to Glacier
        # ...but first ensure user hasn't upgraded in the meantime
        if 'do_not_archive' in ann_item:
          # Get rid of the do_not_archive flag
          # ...and we'll just delete the archive request message
          try:
            ann_item = ann_table.update_item(
              Key={'job_id': job_id},
              UpdateExpression="REMOVE do_not_archive")
          except ClientError as e:
            print(f"Failed to stop job {job_id} from being archived: {e}")

        else:
          glacier = boto3.client('glacier', region_name=config['aws']['AwsRegionName'])
          s3 = boto3.resource('s3', region_name=config['aws']['AwsRegionName'])
          s3_object = s3.Bucket(config['s3']['ResultsBucket']).Object(archive_data['s3_key'])
          # Archive the results file - we read the StreamingBody object direct to Glacier
          try:
            response = glacier.upload_archive(
              vaultName=config['glacier']['Vault'],
              body=s3_object.get()['Body'].read())
          except ClientError as e:
            print(f"Failed to archive results of job {job_id}: {e}")
            return

          # Update DynamoDB item with Glacier archive ID
          try:
            ann_item = ann_table.update_item(
              Key={'job_id': job_id},
              UpdateExpression="SET results_file_archive_id = :rfai",
              ExpressionAttributeValues={':rfai': response['archiveId']})
          except ClientError as e:
            print(f"Failed to save archive ID ({response['archiveId']} for job {job_id}: {e}")
            return

          # Delete results file from S3
          s3_object.delete()
          
          print(f"Archived results file for job ID {job_id}")

        # Delete archive message from queue
        try:
          sqs.delete_message(
            QueueUrl=config['sqs']['ArchiveQueueUrl'],
            ReceiptHandle=message['ReceiptHandle'])
        except ClientError as e:
          print(f"Failed to delete archive message for job {job_id}: {e}")
          return

        print(f"Deleted archive message with ID {message['MessageId']}")

      else:
        # Hide the message until it's time to archive
        try:
          sqs.change_message_visibility(
            QueueUrl=config['sqs']['ArchiveQueueUrl'],
            ReceiptHandle=message['ReceiptHandle'],
            VisibilityTimeout=time_to_archive)
          print(f"Hiding archive message for job {job_id}")
        except ClientError as e:
          print(f"Failed to hide archive message for job {job_id}: {e}")        


if __name__ == '__main__':  
  # Use SQS Client in order to set message visibility
  sqs = boto3.client('sqs', region_name=config['aws']['AwsRegionName'])

  # Check if archive queue exists, otherwise create it
  try:
    sqs.get_queue_url(QueueName=config['sqs']['ArchiveQueueName'])
  except sqs.exceptions.QueueDoesNotExist as err:
    print(f"Queue {config['sqs']['ArchiveQueueName']} not found; creating it...")
    try:
      sqs.create_queue(QueueName=config['sqs']['ArchiveQueueName'])
    except ClientError as e:
      print(f'Failed to create archive queue: {e}')
      sys.exit()

  # Poll queue for new results and process them
  while True:
    handle_archive_queue(sqs=sqs)

### EOF