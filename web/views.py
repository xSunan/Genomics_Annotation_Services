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
    print(key_name)
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

    print("extract succ")
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

    # Get list of annotations to display
    
    return render_template('annotations.html', annotations=None)


"""Display details of a specific annotation job
"""
@app.route('/annotations/<id>', methods=['GET'])
@authenticated
def annotation_details(id):
    pass


"""Display the log file contents for an annotation job
"""
@app.route('/annotations/<id>/log', methods=['GET'])
@authenticated
def annotation_log(id):
    pass


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
        update_profile(
            identity_id=session['primary_identity'],
            role="premium_user"
        )

        # Update role in the session
        session['role'] = "premium_user"

        # Request restoration of the user's data from Glacier
        # Add code here to initiate restoration of archived user data
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