import boto3
import os
from dotenv import load_dotenv
from conductor.client.configuration.configuration import Configuration
from conductor.client.configuration.settings.authentication_settings import AuthenticationSettings
from conductor.client.workflow.conductor_workflow import *
from conductor.client.orkes_clients import OrkesClients
load_dotenv('config.env')

s3_region = 'ap-south-1'
s3_bucket_name = 'orkes-image-input-data'
s3_client = boto3.client('s3',
                        aws_access_key_id='AKIAUTA4UV6WXMYCSA74',
                        aws_secret_access_key='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',
                        region_name='ap-south-1')


def get_configuration():
    envs = {
        'server_api_url': os.getenv('CONDUCTOR_SERVER_URL'),
        'debug': True,
        'authentication_settings': AuthenticationSettings(
            key_id=os.getenv('KEY'),
            key_secret=os.getenv('SECRET')
        )
    }
    return Configuration(**envs)

def list_images_in_s3(bucket_name):
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        if 'Contents' in response:
            return [f"https://{bucket_name}.s3.{s3_region}.amazonaws.com/{obj['Key']}" for obj in response['Contents']]
        else:
            return []
    except Exception as e:
        print(f"Error listing objects in S3 bucket: {e}")
        return []

def main():
    config = get_configuration()
    clients = OrkesClients(config)
    workflow_client = clients.get_workflow_client()
    images = list_images_in_s3(s3_bucket_name)
    for image_url in images:
        request = StartWorkflowRequest(
            version=1,
            name="image_processing_workflow",
            input= {
                "image": image_url
            },
        )
        workflow_id = workflow_client.start_workflow(start_workflow_request=request)
        print(f'Started Workflow id: {workflow_id}')

if __name__ == '__main__':
    main()
    