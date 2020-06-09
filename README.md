# gas-framework
An enhanced web framework (based on [Flask](http://flask.pocoo.org/)) for use in the capstone project. Adds robust user authentication (via [Globus Auth](https://docs.globus.org/api/auth)), modular templates, and some simple styling based on [Bootstrap](http://getbootstrap.com/).

Directory contents are as follows:
* `/web` - The GAS web app files
* `/ann` - Annotator files
* `/util` - Utility scripts for notifications, archival, and restoration
* `/aws` - AWS user data files

## Archive Process

1. Initiate a SNS topic xsunan_results_archive and in run.py publish a message when the job is completed
2. Initiate a SQS named: xsunan_results_archive which subscribe the sns topic xsunan_results_archive. And set the property: delivery delay to 5 minutes, so that the message will be released 5 minutes after the job is completed.
3. In archive.py, accept the message in xsunan_results_archive queue. When a message is received, it means that it was just completed 5 minutes ago. 
   1. Firstly, we need to check if the user is still a free user, in case the user has subscribed after the job is requested
   2. Right at this time, the result of this job should be archived to the Glacier, and the result file in S3 should be deleted, and the item in the dynamodb will be updated

## Restore Process

#### A.Restore

1. Initiate a SNS topic: xsunan_restore. When the user subsribe the premium (/subscribe), publish the message to the SNS topic
2. Initiate a SQS named xsunan_restore which subsribe the SNS topic xsunan_restore. In restore.py, long polling the messages from this queue. And once receive a message, start to restore all the result files of this user.
3. I add a variable in DynamoDB: existed. to indicate whether the result file has been obtained and upladed to S3. In restore.py, I set the exised variable to False.

#### B.Thaw

1. Initiate a SNS topic: xsunan_thaw. In step 2, when we restore the file, we will take the url of the topic-xsunan_thaw as a parameter, then when the restore job is completed, it will automatically public a message to the topic.
2. Initiate a SQS topic: xsunan_thaw which subsribes the SNS topic-xsunan_thaw. In thaw.py, long polling messages from the queue. 
3. Once a message is received, obtain the body of the object and upload it to S3. And update the exsisted variable to true

# Extra Credit:

### 2. file size check

The modification is in views.py and annotate.html

### 13. WEB server Automatic Scale observation: 

![Web Instances Monitor](./readme_images/web_instances.png)



![Web Locust Terminal](./readme_images/web_terminal.png)

### D:

1. The first scale out instance was added as soon as the scale-out alarm has been in alarm. —— This is because the evaluation period I set is 1 minute (and the data point is 1 out of 1 minute). Hence, after a minute's large load, the alarm is in alarm, and the policy is executed.
2.  Since it needs to wait 300 seconds after the last policy, and will need 1 minite to initiate the instance. In this way, the interval is around 6 minutes. 
3. During the time, both the scale in and the scale out policies are triggered in every minute (except at 17:35 only the scale in policy is triggered). When both policies are triggered, AWS follows the scale out policy. In this way, during the time, scale out policy dominates, and causes the increase of instances rather than decreasing every minute. (Reference: https://aws.amazon.com/premiumsupport/knowledge-center/auto-scaling-troubleshooting/)
4. When the number of the instances come to 10, it will not increase any more, since the max number of instances set in the property is 10.
5. There is a decline at around 17:35. It was caused by the scale in policy which was triggered at 17:35 since the response time is below the setting. Then at 17:36 as usual (the interval is 6 minutes, and the last started instance is at 17:30) a new instance is added, so the number of instance increased to 5 again.
   1. The possible reason may be the network bottleneck. Since the scale out of the annotators are quite stable, but the web servers are not stable.

### E

1. After I closed the locust service, the scale out policy will not be triggered, and the scale in policy will work. Since the wait property is not set, and the datapoint is one in a minute, so every minute one instance will be terminated due to the scale in policy.
2. When the number of the instances came to 2, it will not decrease any more, since the minimum number of intances set in the policy is 2.



### 14: Annotator server Automatic Scale observation: 

![image-20200608123907629](./readme_images/ann_monitor.png)

### D

1. At around 10 minutes after send the messages frequently, the scale-out watch will be in alarm (since the we set 1 datapoint in 10 minutes, we need to wait for 10 minutes to collect a datapoint's data). 
2. The scale out policy was firstly actioned as soon as the scale-out alarm has been in alarm. And the instance was successfully added after around 1 minute since it needs some time to initiate.
3. The rest 9 instances were started to initiate every 6 minutes. Since it needs to wait 300 seconds after the last policy, and will need 1 minite to initiate the instance. In this way, the interval is around 6 minutes. 
4. When the number of the instances come to 10, it will not increase any more, since the max number of instances set in the property is 10.

### E

1. After I killed the ann_load.py, the scale out policy will not be triggered, and the scale in policy will work. Since the wait property is not set, and the datapoint is one in a minute, so every minute if the number of sent messages are less than 5, one instance will be terminated due to the scale in policy.
2. In this case, the instances are not decreased every minute. The possible reason maybe:
   1. Jobs are still runing after I stopped sending notification. Since in my implementation I sent arount 4000 notifications, it take a while to complete all the jobs. Hence, even after I stopped sending notification, part of the jobs are still be running. After they completed, they will send out notifications which causes the scale in policy not in alarm. 
3. When the number of the instances came to 2, it will not decrease any more, since the minimum number of intances set in the policy is 2.

## References:

1. configParser: https://docs.python.org/3/library/configparser.html
2. delete queue message: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Client.delete_message
3. DynamoDB 
   1. query: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/dynamodb.html#querying-and-scanning
   2. Update: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.update_item
4. S3
   1. read object: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Object.get
   2. Get object:https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.get_object
   3. delete object: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.delete_object
5. Glacier:
   1. upload object: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier.html#Glacier.Client.upload_archive
   2. initiate job: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier.html#Glacier.Client.initiate_job
6. SNS:
   1. publish: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns.html#SNS.Topic.publish
7. SQS:
   1. https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Client.receive_message