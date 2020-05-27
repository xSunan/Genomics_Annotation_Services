# views.py
#
# Copyright (C) 2011-2020 Vas Vasiliadis
# University of Chicago
#
# Application logic for the GAS
#
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import uuid
import time
import json
from datetime import datetime

import boto3
from boto3.dynamodb.conditions import Key
import botocore
from botocore.client import Config
from botocore.exceptions import ClientError

from flask import (abort, flash, redirect, render_template, 
    request, session, url_for, jsonify)

from gas import app, db
from decorators import authenticated, is_premium
from auth import get_profile, update_profile


"""Start annotation request
Create the required AWS S3 policy document and render a form for
uploading an annotation input file using the policy document.

Note: You are welcome to use this code instead of your own
but you can replace the code below with your own if you prefer.
"""
@app.route('/annotate', methods=['GET'])
@authenticated
def annotate():
    # Create a session client to the S3 service
    s3 = boto3.client('s3', 
        region_name=app.config['AWS_REGION_NAME'],
        config=Config(signature_version='s3v4'))

    bucket_name = app.config['AWS_S3_INPUTS_BUCKET']
    user_id = session['primary_identity']
    # print(user_id)
    # Generate unique ID to be used as S3 key (name)
    key_name = app.config['AWS_S3_KEY_PREFIX'] + user_id + '/' + \
        str(uuid.uuid4()) + '~${filename}'
    # print(key_name)
    # Create the redirect URL
    redirect_url = str(request.url) + '/job'

    # Define policy fields/conditions
    encryption = app.config['AWS_S3_ENCRYPTION']
    acl = app.config['AWS_S3_ACL']
    fields = {
        "success_action_redirect": redirect_url,
        "x-amz-server-side-encryption": encryption,
        "acl": acl
    }
    conditions = [
        ["starts-with", "$success_action_redirect", redirect_url],
        {"x-amz-server-side-encryption": encryption},
        {"acl": acl}
    ]

    # Generate the presigned POST call
    try:
        presigned_post = s3.generate_presigned_post(
            Bucket=bucket_name, 
            Key=key_name,
            Fields=fields,
            Conditions=conditions,
            ExpiresIn=app.config['AWS_SIGNED_REQUEST_EXPIRATION'])
    except ClientError as e:
        app.logger.error(f"Unable to generate presigned URL for upload: {e}")
        return abort(500)
        
    # Render the upload form which will parse/submit the presigned POST
    return render_template('annotate.html', s3_post=presigned_post)


"""Fires off an annotation job
Accepts the S3 redirect GET request, parses it to extract 
required info, saves a job item to the database, and then
publishes a notification for the annotator service.

Note: Update/replace the code below with your own from previous
homework assignments
"""
@app.route('/annotate/job', methods=['GET'])
@authenticated
def create_annotation_job_request():

    # Get bucket name, key, and job ID from the S3 redirect URL
    bucket_name = str(request.args.get('bucket'))
    s3_key = str(request.args.get('key'))

    # Extract the job ID from the S3 key
    try:
        info = s3_key.split("/")
        user = info[1]
        file = info[2]
        job_id = file.split("~")[0]
        input_file = s3_key.split("~")[1]
    except IndexError:
        response = generate_error(500,"No enough information")
        return jsonify(response), 500
        # return abort(500)
    except:
        response = generate_error(500,"No enough information")
        return jsonify(response), 500
        # abort(500)

    # # get user related info
    # profile = get_profile(identity_id=user)
    # email = profile.email
    # name = profile.name

    # print("extract succ ")
    # Persist job to database

    # connect to the dynamodb resource
    try:
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        table_name = app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE']
        ann_table = dynamodb.Table(table_name)
    except boto3.exceptions.ResourceNotExistsError as e:
        response = generate_error(500,"ResourceNotExistsError")
        return jsonify(response), 500
        # return abort(500)
    except botocore.exceptions.ClientError as e:
        response = generate_error(500,"ClientError")
        return jsonify(response), 500

    ep_time = int(time.time())
    data = { 
        "job_id": job_id,
        "user_id": user,
        # "user_email": email,
        # "user_name": name,
        "input_file_name":input_file,
        "s3_inputs_bucket": bucket_name,
        "s3_key_input_file": s3_key,
        "submit_time":ep_time,
        "job_status": "PENDING"
    }
    data_str = json.dumps(data)

    # put the item into table
    try:
        ann_table.put_item(Item=data)
    except botocore.errorfactory.ResourceNotFoundException as e:
        # response = generate_error(500, e.response['Error']['Message'])
        # return jsonify(response), 500
        return abort(500)
    except botocore.exceptions.ClientError as e:
        # response = generate_error(500, e.response['Error']['Message'])
        # return jsonify(response), 500
        return abort(500)

    # Send message to request queue
    try:
        sns = boto3.resource('sns', region_name='us-east-1')
    except boto3.exceptions.ResourceNotExistsError as e:
        # response = generate_error(500, e)
        # return jsonify(response), 500
        return abort(500)

    try:
        # topic = sns.Topic('arn:aws:sns:us-east-1:127134666975:xsunan_job_requests')
        topic_name = app.config['AWS_SNS_JOB_REQUEST_TOPIC']
        topic = sns.Topic(topic_name)
    except (botocore.errorfactory.NotFoundException, botocore.errorfactory.InvalidParameterException) as e:
        # response = generate_error(500, e)
        # return jsonify(response), 500
        return abort(500)
    
    # publish the notification
    try:
        response = topic.publish(
            Message= data_str,
            MessageStructure='String',
        )
    except (botocore.exceptions.ParamValidationError,botocore.exceptions.ClientError) as e:
        # response = generate_error(500, e)
        # return jsonify(response), 500
        return abort(500)


    return render_template('annotate_confirm.html', job_id=job_id)


"""List all annotations for the user
"""
@app.route('/annotations', methods=['GET'])
@authenticated
def annotations_list():
    # connect to the dynamoDB
    try:
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        table_name = app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE']
        ann_table = dynamodb.Table(table_name)
    except boto3.exceptions.ResourceNotExistsError as e:
        response = generate_error(500,"ResourceNotExistsError")
        return jsonify(response), 500
        # return abort(500)
    except botocore.exceptions.ClientError as e:
        response = generate_error(500,"ClientError")
        return jsonify(response), 500

    # get the authorized user
    user_id = session['primary_identity']

    # Get list of annotations to display
    try:
        response = ann_table.query(
            IndexName = 'user_id_index',
            KeyConditionExpression=Key('user_id').eq(user_id)
        )
    except ClientError:
        abort(500)

    items = response['Items']
    for job in items:
        job['submit_time'] = datetime.fromtimestamp(job['submit_time'])
    
    return render_template('annotations.html', annotations=items)


"""Display details of a specific annotation job
"""
@app.route('/annotations/<id>', methods=['GET'])
@authenticated
def annotation_details(id):
    # connect to the dynamoDB
    try:
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        table_name = app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE']
        ann_table = dynamodb.Table(table_name)
    except boto3.exceptions.ResourceNotExistsError as e:
        response = generate_error(500,"ResourceNotExistsError")
        return jsonify(response), 500
        # return abort(500)
    except botocore.exceptions.ClientError as e:
        response = generate_error(500,"ClientError")
        return jsonify(response), 500
    
    response = ann_table.query(
        KeyConditionExpression=Key('job_id').eq(id)
    )
    job = response['Items'][0]

    # check the job belongs to the authorized user
    user_id = session['primary_identity']
    profile = get_profile(identity_id = user_id)

    if job['user_id'] != user_id:
        return render_template('error.html',
        title='Not authorized', alert_level='danger',
        message="You are not authorized to view this job. \
            If you think you deserve to be granted access, please contact the \
            supreme leader of the mutating genome revolutionary party."
        ), 403
        # abort(403)

    job['submit_time'] = datetime.fromtimestamp(job['submit_time'])
    free_access_expired = False
    if job['job_status'] == 'COMPLETED':
        if profile.role == 'free_user':       # if the user is a free_user, check if free access has passed
            cur_time = int(time.time())
            # if passed, set the free_access_expired as true
            if cur_time-job['complete_time'] > 300:
                free_access_expired = 1
        else:                                 # if a premium user, check if the result file has been successfully restored
            if "existed" in job and job['existed'] == 'False':
                job['restore_message'] = "Result File being Restoring, Please wait"

        job['complete_time'] = datetime.fromtimestamp(job['complete_time'])
        job['result_file_url'] = create_presigned_download_url(job['s3_key_result_file'])

    return render_template('annotation_details.html', annotation=job,free_access_expired=free_access_expired)

def create_presigned_download_url(result_object_key):
    # Create a session client to the S3 service
    s3 = boto3.client('s3', 
        region_name=app.config['AWS_REGION_NAME'],
        config=Config(signature_version='s3v4'))

    result_bucket = app.config['AWS_S3_RESULTS_BUCKET']

    try:
        response = s3.generate_presigned_url('get_object',
            Params={
                    'Bucket': result_bucket,
                    'Key': result_object_key
                    },
            ExpiresIn=app.config['AWS_SIGNED_REQUEST_EXPIRATION']
        )
    except ClientError as e:
        app.logger.error(f"Unable to generate presigned URL for upload: {e}")
        return None

    return response

"""Display the log file contents for an annotation job
"""
@app.route('/annotations/<id>/log', methods=['GET'])
@authenticated
def annotation_log(id):
     # connect to the dynamoDB
    try:
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        table_name = app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE']
        ann_table = dynamodb.Table(table_name)
    except boto3.exceptions.ResourceNotExistsError as e:
        response = generate_error(500,"ResourceNotExistsError")
        return jsonify(response), 500
        # return abort(500)
    except botocore.exceptions.ClientError as e:
        response = generate_error(500,"ClientError")
        return jsonify(response), 500
    
    response = ann_table.query(
        KeyConditionExpression=Key('job_id').eq(id)
    )
    job = response['Items'][0]

    # check the job belongs to the authorized user
    user_id = session['primary_identity']
    if job['user_id'] != user_id:
        return render_template('error.html',
        title='Not authorized', alert_level='danger',
        message="You are not authorized to view this job. \
            If you think you deserve to be granted access, please contact the \
            supreme leader of the mutating genome revolutionary party."
        ), 403
        # abort(403)

    # retrieve the log file's s3 key
    s3_key = job['s3_key_log_file']

    # Create a session client to the S3 service
    s3 = boto3.resource('s3', 
        region_name=app.config['AWS_REGION_NAME'])
    bucket_name = app.config['AWS_S3_RESULTS_BUCKET']

    # get the log file's object and read it
    try:
        object = s3.Object(bucket_name,s3_key)
        content = object.get()['Body'].read().decode()
    except botocore.exceptions.ClientError as e:
        print(e)

    return render_template('view_log.html', log_file_contents = content, job_id = id)

"""Subscription management handler
"""
import stripe

@app.route('/subscribe', methods=['GET', 'POST'])
@authenticated
def subscribe():
    if (request.method == 'GET'):
        # Display form to get subscriber credit card info
        if (session.get('role') == "free_user"):
            return render_template('subscribe.html')
        else:
            return redirect(url_for('profile'))

    elif (request.method == 'POST'):
        # Process the subscription request
        token = str(request.form['stripe_token']).strip()
        # print(token)
        # Create a customer on Stripe
        stripe.api_key = app.config['STRIPE_SECRET_KEY']
        try:
            customer = stripe.Customer.create(
                card = token,
                plan = "premium_plan",
                email = session.get('email'),
                description = session.get('name')
            )
        except Exception as e:
            app.logger.error(f"Failed to create customer billing record: {e}")
            return abort(500)

        # Update user role to allow access to paid features
        user_id = session['primary_identity']
        update_profile(
            identity_id=user_id,
            role="premium_user"
        )

        # Update role in the session
        session['role'] = "premium_user"

        # Request restoration of the user's data from Glacier
        # Add code here to initiate restoration of archived user data
        # Send message to request queue
        try:
            sns = boto3.resource('sns', region_name=app.config['AWS_REGION_NAME'])
            topic_name = app.config['AWS_SNS_RESTORE_TOPIC']
            restore_topic = sns.Topic(topic_name)
            # print("send message")
        except (botocore.errorfactory.NotFoundException, botocore.errorfactory.InvalidParameterException) as e:
            return abort(500)
        
        restore_notification = {
            "user_id": user_id
        }
        # print(restore_notification)
        # publish the notification
        try:
            response = restore_topic.publish(
                Message= json.dumps(restore_notification),
                MessageStructure='String',
            )
            # print(response)
        except (botocore.exceptions.ParamValidationError,botocore.exceptions.ClientError) as e:
            return abort(500)
        # Make sure you handle files not yet archived!

        # Display confirmation page
        return render_template('subscribe_confirm.html', 
            stripe_customer_id=str(customer['id']))


"""Reset subscription
"""
@app.route('/unsubscribe', methods=['GET'])
@authenticated
def unsubscribe():
    # Hacky way to reset the user's role to a free user; simplifies testing
    update_profile(
        identity_id=session['primary_identity'],
        role="free_user"
    )
    return redirect(url_for('profile'))


"""DO NOT CHANGE CODE BELOW THIS LINE
*******************************************************************************
"""

"""Home page
"""
@app.route('/', methods=['GET'])
def home():
    return render_template('home.html')

"""Login page; send user to Globus Auth
"""
@app.route('/login', methods=['GET'])
def login():
    app.logger.info(f"Login attempted from IP {request.remote_addr}")
    # If user requested a specific page, save it session for redirect after auth
    if (request.args.get('next')):
        session['next'] = request.args.get('next')
    return redirect(url_for('authcallback'))

"""404 error handler
"""
@app.errorhandler(404)
def page_not_found(e):
    return render_template('error.html', 
        title='Page not found', alert_level='warning',
        message="The page you tried to reach does not exist. \
            Please check the URL and try again."
        ), 404

"""403 error handler
"""
@app.errorhandler(403)
def forbidden(e):
    return render_template('error.html',
        title='Not authorized', alert_level='danger',
        message="You are not authorized to access this page. \
            If you think you deserve to be granted access, please contact the \
            supreme leader of the mutating genome revolutionary party."
        ), 403

"""405 error handler
"""
@app.errorhandler(405)
def not_allowed(e):
    return render_template('error.html',
        title='Not allowed', alert_level='warning',
        message="You attempted an operation that's not allowed; \
            get your act together, hacker!"
        ), 405

"""500 error handler
"""
@app.errorhandler(500)
def internal_error(error):
    return render_template('error.html',
        title='Server error', alert_level='danger',
        message="The server encountered an error and could \
            not process your request."
        ), 500

    
def generate_error(status_code,e):
    '''
    construct response for errors
    '''
    response_body = {
        "code": status_code,
        "status": "error",
        "message": str(e)
    }
    return response_body
### EOF