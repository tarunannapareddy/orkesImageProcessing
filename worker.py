from conductor.client.http.models.task import Task
from conductor.client.http.models.task_result import TaskResult
from conductor.client.http.models.task_exec_log import TaskExecLog
from conductor.client.http.models.task_result_status import TaskResultStatus
import os
import boto3
from urllib.parse import urlparse
import socket
from PIL import Image
from PIL import ImageFilter
import random

output_bucket = 'orkes-image-data'
def extract_bucket_and_filename(s3_link):
    parsed_url = urlparse(s3_link)
    if parsed_url.scheme != "https":
        raise ValueError("Not an S3 link")
    
    bucket_name = parsed_url.netloc.split('.')[0]
    file_path, file_name = os.path.split(parsed_url.path.lstrip('/'))
    return bucket_name, file_path, file_name

s3 = boto3.client('s3',
                        aws_access_key_id='',
                        aws_secret_access_key='',
                        region_name='ap-south-1')

def download_image_from_s3(bucket_name, file_path, file_name):
    local_file_path = os.path.join('/tmp', file_name)  # Save to a temporary directory
    s3.download_file(bucket_name, f"{file_name}", local_file_path)
    return local_file_path

def rotate_image(task: Task) -> TaskResult:
     task_result = to_task_result(task)
     image_link = task.input_data['image']
     bucket_name, file_path, inputfile = extract_bucket_and_filename(image_link) 
     local_file_path = download_image_from_s3(bucket_name, file_path, inputfile)
     
     with Image.open(local_file_path) as img:
            rotated_image = img.rotate(90, expand=True)
            outputfile = f"rotated_{inputfile}"
            rotated_file_path = os.path.join('/tmp', outputfile)
            rotated_image.save(rotated_file_path)
            s3.upload_file(rotated_file_path, output_bucket, outputfile)
     
     task_result.status = TaskResultStatus.COMPLETED
     task_result.add_output_data('image', f"https://{output_bucket}.s3.ap-south-1.amazonaws.com/{outputfile}")
     return task_result

def flip_image(task: Task) -> TaskResult:
    task_result = to_task_result(task)
    image_link = task.input_data['image']
    flip_direction = 'horizontal'  # Default flip direction
    bucket_name, file_path, inputfile = extract_bucket_and_filename(image_link)
    local_file_path = download_image_from_s3(bucket_name, file_path, inputfile)
    
    try:
        with Image.open(local_file_path) as img:
            flipped_image = img.transpose(Image.FLIP_LEFT_RIGHT)
            outputfile = f"flipped_{inputfile}"
            flipped_file_path = os.path.join('/tmp', outputfile)
            flipped_image.save(flipped_file_path)
            s3.upload_file(flipped_file_path, output_bucket, outputfile)
        
        task_result.status = TaskResultStatus.COMPLETED
        task_result.add_output_data('image', f"https://{output_bucket}.s3.ap-south-1.amazonaws.com/{outputfile}")
    except Exception as e:
        task_result.status = TaskResultStatus.FAILED
        print(f"Error processing image: {e}")
    
    return task_result


def blur_image(task: Task) -> TaskResult:
    task_result = to_task_result(task)
    image_link = task.input_data['image']
    blur_radius = 5  # Default blur radius
    bucket_name, file_path, inputfile = extract_bucket_and_filename(image_link)
    local_file_path = download_image_from_s3(bucket_name, file_path, inputfile)
    
    try:
        with Image.open(local_file_path) as img:
            blurred_image = img.filter(ImageFilter.GaussianBlur(blur_radius))
            outputfile = f"blurred_{inputfile}"
            blurred_file_path = os.path.join('/tmp', outputfile)
            blurred_image.save(blurred_file_path)
            s3.upload_file(blurred_file_path, output_bucket, outputfile)
        
        task_result.status = TaskResultStatus.COMPLETED
        task_result.add_output_data('image', f"https://{output_bucket}.s3.ap-south-1.amazonaws.com/{outputfile}")
    except Exception as e:
        print(f"Error processing image: {e}")
        task_result.status = TaskResultStatus.FAILED
    
    return task_result

def crop_image(task: Task) -> TaskResult:
    task_result = to_task_result(task)
    image_link = task.input_data['image']
    bucket_name, file_path, inputfile = extract_bucket_and_filename(image_link)
    local_file_path = download_image_from_s3(bucket_name, file_path, inputfile)
    
    try:
        with Image.open(local_file_path) as img:
            width, height = img.size
            left = int(0.01 * width)
            top = int(0.01 * height)
            right = width - left
            bottom = height - top
            crop_box = (left, top, right, bottom)
            cropped_image = img.crop(crop_box)
            outputfile = f"cropped_{inputfile}"
            cropped_file_path = os.path.join('/tmp', outputfile)
            cropped_image.save(cropped_file_path)
            s3.upload_file(cropped_file_path, output_bucket, outputfile)
        
        task_result.status = TaskResultStatus.COMPLETED
        task_result.add_output_data('image', f"https://{output_bucket}.s3.ap-south-1.amazonaws.com/{outputfile}")
    except Exception as e:
        print(f"Error processing image: {e}")
        task_result.status = TaskResultStatus.FAILED
    
    return task_result


def to_task_result(task: Task) -> TaskResult:
    return TaskResult(
        task_id=task.task_id,
        workflow_instance_id=task.workflow_instance_id,
        worker_id=socket.gethostname(),
        logs=[],
    )
