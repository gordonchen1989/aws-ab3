import re
import os
import os
import boto3
import json
from botocore.exceptions import ClientError
import glob

SNS_TOPIC_ARN = 'arn:aws:sns:us-east-1:941797585610:RekonitionTopic'
S3_DESTINATION_BUCKET = "my-ivs-archive-202107"
S3_PHOTO_BUCKET = 'gordon-private-bucket-2021'
S3_PHOTO_PREFIX = 'rekognition_photo/'
SIGNED_URL_TIMEOUT = 60
PERIOD = 3
autosuspended_labels = ['suggestive']


def notify_admin(message):
    client = boto3.client('sns')
    print(message)
    response = client.publish(
        TargetArn=SNS_TOPIC_ARN,
        Message=json.dumps(message)
    )
    print(f'sns response: {response}')
    return response

def create_moderation_job(bucket, key_list):
    """ Generate content moderation job """

    client = boto3.client('rekognition')
    response_list = []
    for key in key_list:
        response = client.detect_moderation_labels(
            Image={
                'S3Object': {
                    'Bucket': bucket,
                    'Name': key
                }
            },
            MinConfidence=90
        )
        response_list.append(response['ModerationLabels'])
    return response_list
    
def check_moderate_policies(result_list):
    """ Strip out the space in the response and prepare the content """

    # Status change decides whether the channel needs to be suspend quickly
    # or needs to be passed to the moderation queue
    # suspend = 'suspend' / suspend = 'moderate'

    status = []
    mod_results = {}
    for result in result_list:
        for label in result:
            modified_label = label['Name'].replace(" ", "_").lower()
            print(modified_label)
            # log.debug("Label %s translated to %s", label, modified_label)
    
            if modified_label in autosuspended_labels:
                if (label['Confidence'] >= 90):
                    status.append(modified_label)
                        # log.info('Label: %s, is in autosuspended list, %s', label['Name'], label['Confidence'])
                # else:
                    # log.info("The label %s is below autosuspend threshold, %s", label['Name'], label['Confidence'])
            
            # elif modified_label in autosuspended_labels and autosuspended_labels[modified_label] != '0': 
                # log.info("Auto suspension is disabled for label %s", modified_label)
            else:
                print("Label %s is not defined in auto suspension list.", modified_label)
    
    print(status)
    return status

def lambda_handler(event, context):
    # TODO implement
    s3_source_bucket = event['Records'][0]['s3']['bucket']['name']
    s3_source_key = event['Records'][0]['s3']['object']['key']
    #产生sign url以供下载ts
    s3_client = boto3.client('s3')
    s3_source_signed_url = s3_client.generate_presigned_url('get_object',
        Params={'Bucket': s3_source_bucket, 'Key': s3_source_key},
        ExpiresIn=SIGNED_URL_TIMEOUT)
    
    print(f'file path: {s3_source_key}')
    if s3_source_key.split('/')[-2] == '480p30':
        ts_name = s3_source_key.split('/')[-1]
        ts_num = ts_name.split('.')[1]
        channel_id = s3_source_key.split('/')[3]
        stream_id = s3_source_key.split('/')[9]
        try:
            s3_client.download_file(s3_source_bucket, s3_source_key, f'/tmp/{ts_name}')
        except ClientError as e:
            print(e)            
        # os.system('ls -l /tmp/')
        # os.system('ls -l /opt/')
        # os.system('ls -l /opt/bin/')
        
        ffmpeg_cmd = f'/opt/bin/ffmpeg -i /tmp/{ts_name} -vf "select=(gte(t\,{PERIOD}))*(isnan(prev_selected_t)+gte(t-prev_selected_t\,{PERIOD}))" -vsync 0 /tmp/{channel_id}-{stream_id}-{ts_num}-image_%05d.jpg'
        print(ffmpeg_cmd)
        os.system(ffmpeg_cmd)
        os.system('ls -l /tmp/')
        file_regx = f'/tmp/{channel_id}-{stream_id}-{ts_num}-*'
        s3_photo_list = []
        for name in glob.glob(file_regx):
            print(name)
            s3_destination_filename = S3_PHOTO_PREFIX + name.split('/')[2]
            print(s3_destination_filename)
            s3_photo_list.append(s3_destination_filename)
            print(s3_photo_list)
            try:
                response = s3_client.upload_file(name, S3_PHOTO_BUCKET, s3_destination_filename)
                print(response)
            except Exception as e:
                print(e)
                
        result_list = create_moderation_job(S3_PHOTO_BUCKET, s3_photo_list) 
        print(result_list)
        moderate_result =  check_moderate_policies(result_list)
        if len(moderate_result) > 0:
            notify_admin(f'message: {channel_id} mark as {moderate_result[0]}')
        return {
            'statusCode': 200,
            'body': json.dumps(f'success! cmd: {ffmpeg_cmd}')
        }
    else:
        return {
            'statusCode': 200,
            'body': json.dumps('not 480p30')
        }

