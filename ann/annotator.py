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
        sqs = boto3.client('sqs', region_name=config['aws']['AwsRegionName'])
        queue_url = config['aws']['SQS_REQUESTS_URL']
        s3 = boto3.resource('s3')
    except boto3.exceptions.ResourceNotExistsError as e:
        print(e)

    # connect to dynamoDB
    try:
        dynamodb = boto3.resource('dynamodb', region_name=config['aws']['AwsRegionName'])
        table_name = config['aws']['DynamoTableName']
        ann_table = dynamodb.Table(table_name)
    except boto3.exceptioƒns.ResourceNotExistsError as e:
        print("ResourceNotExistsError")
    except botocore.exceptions.ClientError as e:
        print("ClientError")

    while True:
        # long polling the messages with wait time seconds set to 20s
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Client.receive_message
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
            continue

        # if no messaged obtained
        try:
            messages = job_response['Messages']
        except KeyError:
            continue

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
                user_id = data['user_id']
                key = data['s3_key_input_file']
                bucket = data['s3_inputs_bucket']
                submit_time = data['submit_time']
                input_file = data['input_file_name']
            except json.JSONDecodeError:
                print('Error: Input is not valid json format')
                delete_message(sqs, queue_url, receipt_handle)
                continue
            except KeyError:
                print("Error: Input doesn't have corresponding key")
                delete_message(sqs, queue_url, receipt_handle)
                continue

            
            # prepare the file directory for the job
            if not os.path.isdir("./{}/".format(user_id)):
                os.mkdir("./{}/".format(user_id))

            dst = './{}/{}/'.format(user_id,job_id)

            try:
                os.mkdir(dst)
            except FileExistsError as e:
                delete_message(sqs, queue_url, receipt_handle)
                print(e)

            try:
                folder = s3.Bucket(bucket)
            except botocore.errorfactory.NoSuchBucket as e:
                print(e.response['Error']['Message'])
                delete_message(sqs, queue_url, receipt_handle)
                continue

            # validate the type of the file
            if not input_file.endswith('.vcf'):
                print("The uploaded file should be .vcf")
                delete_message(sqs, queue_url, receipt_handle)
                continue
                
            filename = dst+job_id+"~"+input_file

            # Get the input file S3 object and copy it to a local file
            # https://boto3.amazonaws.com/v1/documentation/api/1.9.42/guide/s3-example-download-file.html 
            try:
                s3.Bucket(bucket).download_file(key, filename)
            except botocore.exceptions.ClientError as e:
                print("An error occurred (403) when downloading {}".format(key))
                delete_message(sqs, queue_url, receipt_handle)
                continue
            except FileNotFoundError as e:
                print(e)
                delete_message(sqs, queue_url, receipt_handle)
                continue

            # Launch annotation job as a background process
            # https://docs.python.org/3/library/subprocess.html#popen-constructor
            try:           
                execFile = "./run.py"
                job = Popen(["python", execFile, filename])
            except (FileNotFoundError, ValueError, OSError) as e:
                print(e)
                delete_message(sqs, queue_url, receipt_handle)
                continue
            except:
                print("Unexpected Error", sys.exc_info()[0])
                delete_message(sqs, queue_url, receipt_handle)
                continue

            # update the job_status in dynamoDB    
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.update_item
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
                delete_message(sqs, queue_url, receipt_handle)
                continue
            except:
                print('Unexpected Error'+sys.exc_info()[0])
                delete_message(sqs, queue_url, receipt_handle)
                continue

            delete_message(sqs, queue_url, receipt_handle)

def delete_message(sqs, queue_url, receipt_handle):
    # delete the message
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Client.delete_message
    dele_rsp = sqs.delete_message(
        QueueUrl = queue_url,
        ReceiptHandle = receipt_handle
    )           

def main():
    create_job()

if __name__ == "__main__":
    main()

