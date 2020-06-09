import uuid
import time
import json
from datetime import datetime
import os
import boto3
import botocore

from configparser import SafeConfigParser
config = SafeConfigParser(os.environ)
config.read(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'util_config.ini'))


def send_message():
    # Send message to request queue
    job_id = str(uuid.uuid4())
    input_file_name = "test.vcf"
    user_id = "c59c7105-29d3-4f98-8c5a-88ea39b684db"
    bucket_name = config['aws']['INPUT_BUCKET_NAME']
    s3_key = "xsunan/"+user_id+"/"+job_id+"~"+input_file_name
    ep_time = int(time.time())

    data = { 
        "job_id": job_id,
        "user_id": user_id,
        "input_file_name":input_file_name,
        "s3_inputs_bucket": bucket_name,
        "s3_key_input_file": s3_key,
        "submit_time":ep_time,
        "job_status": "PENDING"
    }
    data_str = json.dumps(data)

    try:
        sns = boto3.resource('sns', region_name=config['aws']['AwsRegionName'])
    except boto3.exceptions.ResourceNotExistsError as e:
        print("cannot connect to sns")
        exit(1)

    try:
        topic_name = config['aws']['AWS_SNS_JOB_REQUEST_TOPIC']
        topic = sns.Topic(topic_name)
    except (botocore.errorfactory.NotFoundException, botocore.errorfactory.InvalidParameterException) as e:
        print("cannot connect to the topic")
        exit(1)
    
    # publish the notification
    try:
        response = topic.publish(
            Message= data_str,
            MessageStructure='String',
        )
    except (botocore.exceptions.ParamValidationError,botocore.exceptions.ClientError) as e:
        print("cannot publish the message")
        exit(1)
    print("successfully publish")

def main():
    while True:
        time.sleep(10)
        send_message()

if __name__ == '__main__':
    main()
