from flask import Flask
from flask import request
from flask import jsonify
from uuid import uuid4
from subprocess import Popen, PIPE
from boto3.dynamodb.conditions import Key
import botocore
import json 
import os
import shutil
import boto3
from configparser import ConfigParser


def create_job():
    # Get configuration
    config = ConfigParser()
    config.read('ann_config.ini')
    
    # Connect to SQS and get the message queue, and connect to s3
    try:
        sqs = boto3.client('sqs', region_name='us-east-1')
        queue_url = config['aws']['SqsURL']
        s3 = boto3.resource('s3')
    except boto3.exceptions.ResourceNotExistsError as e:
        print(e)

    # connect to dynamoDB
    try:
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        table_name = config['aws']['DynamoTableName']
        ann_table = dynamodb.Table(table_name)
    except boto3.exceptioƒns.ResourceNotExistsError as e:
        print("ResourceNotExistsError")
    except botocore.exceptions.ClientError as e:
        print("ClientError")

    while True:
        # long polling the messages with wait time seconds set to 20s
        try:
            job_response = sqs.receive_message(
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

        # if no messaged obtained
        try:
            messages = job_response['Messages']
        except KeyError:
            continue;

        if len(messages)==0:
            continue

        # retrieve the info about job
        for message in messages:
            try:
                content = message['Body']
                receipt_handle = message['ReceiptHandle']
                body = json.loads(content)
                data = json.loads(body['Message'])
                job_id = data['job_id']
                username = data['user_id']
                key = data['s3_key_input_file']
                bucket = data['s3_inputs_bucket']
                submit_time = data['submit_time']
                input_file = data['input_file_name']
            except json.JSONDecodeError:
                print('Error: Input is not valid json format')
                continue
            except KeyError:
                print("Error: Input doesn't have corresponding key")
                continue

            # delete the message
            dele_rsp = sqs.delete_message(
                QueueUrl = queue_url,
                ReceiptHandle = receipt_handle
            )
            # prepare the file directory for the job
            if not os.path.isdir("./{}/".format(username)):
                os.mkdir("./{}/".format(username))

            dst = './{}/{}/'.format(username,job_id)

            try:
                os.mkdir(dst)
            except FileExistsError as e:
                print(e)

            # filter out and download the file in s3 bucket
            try:
                folder = s3.Bucket(bucket)
            except botocore.errorfactory.NoSuchBucket as e:
                print(e.response['Error']['Message'])

            # validate the type of the file
            if not input_file.endswith('.vcf'):
                print("The uploaded file should be .vcf")
                
            filename = dst+job_id+"~"+input_file

            # Get the input file S3 object and copy it to a local file 
            try:
                s3.Bucket(bucket).download_file(key, filename)
            except botocore.exceptions.ClientError as e:
                print("An error occurred (403) when downloading {}".format(key))
            except FileNotFoundError as e:
                print(e)

            # Launch annotation job as a background process
            try:           
                execFile = "./anntools/run.py"
                job = Popen(["python", execFile, filename])
            except (FileNotFoundError, ValueError, OSError) as e:
                print(e)
                continue
            except:
                print("Unexpected Error", sys.exc_info()[0])
                continue

            # update the job_status in dynamoDB    
            
            try:
                response = ann_table.update_item(
                    Key={
                        'job_id': job_id
                        },
                    UpdateExpression="set job_status = :r",
                    ExpressionAttributeValues={
                        ':r': "RUNNING",
                        ':p': "PENDING"
                    },
                    ConditionExpression=" job_status = :p",
                    ReturnValues="UPDATED_NEW"

                )
            except (botocore.errorfactory.ConditionalCheckFailedException,botocore.exceptions.ClientError) as e:
                print(e.response['Error']['Message'])
                continue
            except:
                print('Unexpected Error'+sys.exc_info()[0])
                continue

            

def main():
    create_job()

if __name__ == "__main__":
    main()

