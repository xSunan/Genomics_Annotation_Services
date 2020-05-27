# run.py
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
#
# Wrapper script for running AnnTools
#
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import sys
import time
import driver
from boto3.dynamodb.conditions import Key
import boto3
import json
import time
import os
import shutil
import botocore
from pprint import pprint
from configparser import ConfigParser

"""A rudimentary timer for coarse-grained profiling
"""
class Timer(object):
    def __init__(self, verbose=True):
        self.verbose = verbose

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.secs = self.end - self.start
        if self.verbose:
            print(f"Approximate runtime: {self.secs:.2f} seconds")

def upload_result(file_name):
    '''
    upload the result files to S3 storage
    '''
    # obtain the config
    config = ConfigParser()
    config.read('ann_config.ini')

    # retrieve info from filename
    s3 = boto3.resource('s3')
    bucket = config['aws']['AWS_S3_RESULTS_BUCKET']
    try:
        user_id = file_name.split("/")[1]
        prefix = file_name[2:len(file_name)-4]
        job_id = file_name.split("/")[2]
        suffix = prefix.split("/")[2]

    except IndexError as e:
      print("File name not valid\n"+e)
      return
    
    # load user related info
    # user_info_file = "{}/{}/user_info.txt".format(user_id, job_id)
    # print(user_info_file)
    # with open(user_info_file,"r") as f:
    #     user_info = json.load(f)
    #     user_name = user_info['name']
    #     user_email = user_info['email']
    #     print(user_name, user_email)
     
    #upload the final output to s3
    s3_prefix = config['aws']['AWS_S3_KEY_PREFIX']
    annot_file = '{}.annot.vcf'.format(prefix)
    log_file = '{}.vcf.count.log'.format(prefix)
    annot_key = '{}{}/{}.annot.vcf'.format(s3_prefix,user_id,suffix)
    log_key = '{}{}/{}.vcf.count.log'.format(s3_prefix,user_id,suffix)
    
    try:
        s3.meta.client.upload_file(annot_file, bucket, annot_key)
        s3.meta.client.upload_file(log_file, bucket, log_key)
    except (FileNotFoundError, boto3.exceptions.S3UploadFailedError) as e:
        print(e)
        return

    # delete the output file in instance
    try:
        shutil.rmtree("./{}/{}".format(user_id,job_id))
    except FileNotFoundError as e:
        print(e)
    # return log_key, annot_key, job_id,user_id,user_name,user_email
    return log_key, annot_key, job_id,user_id

if __name__ == '__main__':
    # Call the AnnTools pipeline
    if len(sys.argv) > 1:
        file_name = sys.argv[1]
        job_complete = True
        with Timer():
            try:
                driver.run(sys.argv[1], 'vcf')
            except :
                job_complete = False
            complete_time = int(time.time())

        # upload the log and count file to gas-results
        log_key, annot_key, job_id,user_id = upload_result(file_name)

        # obtain the config
        config = ConfigParser()
        config.read('ann_config.ini')

        # update the job_status and add more info in dynamoDB
        try:
            dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
            table_name = config['aws']['DynamoTableName']
            ann_table = dynamodb.Table(table_name)
        except boto3.exceptions.ResourceNotExistsError as e:
            print("ResourceNotExistsError")
        except botocore.exceptions.ClientError as e:
            print("ClientError")
        
        if job_complete :
            # update the status to complete and add log and result files' key
            try:
                response = ann_table.update_item(
                    Key={
                        'job_id': job_id
                        },
                    UpdateExpression="set job_status = :r , \
                    s3_results_bucket=:b, s3_key_log_file=:l, \
                    s3_key_result_file=:a, complete_time=:t" ,
                    ExpressionAttributeValues={
                        ':r': "COMPLETED",
                        ':a': annot_key,
                        ':l': log_key,
                        ':b': "gas-results",
                        ':t': complete_time
                    },
                    # ConditionExpression=" job_status = :p",
                    ReturnValues="UPDATED_NEW"

                )
            except botocore.errorfactory.ConditionalCheckFailedException as e:
                print("Doesn't meet the ConditionExpressions") 
            except botocore.exceptions.ClientError as e:
                print(e.response['Error']['Message'])
            except:
                print("Unexpected error")

            # connect to the sns and topic
            try:
                sns = boto3.resource('sns', region_name='us-east-1')
                topic_result_name = config['aws']['SNS_RESULT_TOPIC']
                topic_archive_name = config['aws']['SNS_ARCHIVE_TOPIC']
                topic_result = sns.Topic(topic_result_name)
                topic_archive = sns.Topic(topic_archive_name)
            except (botocore.errorfactory.NotFoundException, botocore.errorfactory.InvalidParameterException, \
                boto3.exceptions.ResourceNotExistsError) as e:
                print(e)
                exit(1)


            ep_time = int(time.time())
            # construct the notification
            result_notification = { 
                "job_id": job_id,
                "user_id": user_id,
                # "user_name": user_name,
                # "user_email": user_email,
                "s3_results_bucket": config['aws']['AWS_S3_RESULTS_BUCKET'],
                "s3_key_log_file": log_key,
                "s3_key_annot_file": annot_key,
                "completed_time":ep_time,
            }

            archive_notification = {
                "job_id": job_id,
                "user_id": user_id,
                # "user_name": user_name,
                # "user_email": user_email,
                "s3_results_bucket": config['aws']['AWS_S3_RESULTS_BUCKET'],
                "s3_key_log_file": log_key,
                "s3_key_annot_file": annot_key,
                "completed_time":ep_time,
            }

            # publish a notification message to job_result and result_archive SNS when job is complete
            try:
                result_response = topic_result.publish(
                    Message= json.dumps(result_notification),
                    MessageStructure='String',
                )
                archive_response = topic_archive.publish(
                    Message= json.dumps(archive_notification),
                    MessageStructure='String',
                )
            except (botocore.exceptions.ParamValidationError,botocore.exceptions.ClientError) as e:
                print(e)
                exit(1)

        else :
            # update the status to error
            try:
                response = ann_table.update_item(
                    Key={
                        'job_id': job_id
                        },
                    UpdateExpression="set job_status = :r" ,
                    ExpressionAttributeValues={
                        ':r': "ERROR"
                    },
                    # ConditionExpression=" job_status = :p",
                    ReturnValues="UPDATED_NEW"

                )
            except botocore.errorfactory.ConditionalCheckFailedException as e:
                print("Doesn't meet the ConditionExpressions")
            except botocore.exceptions.ClientError as e:
                print(e.response['Error']['Message'])
            except:
                print("Unexpected error")

    else:
        print("A valid .vcf file must be provided as input to this program.")

### EOF