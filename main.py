import os
from dotenv import load_dotenv
from conductor.client.automator.task_handler import TaskHandler
from typing import Dict
from conductor.client.configuration.configuration import Configuration
from conductor.client.configuration.settings.authentication_settings import AuthenticationSettings
from conductor.client.worker.worker import Worker
from worker import *

load_dotenv('config.env')


def main():
    task_handler = start_workers()
    task_handler.join_processes()


def start_workers() -> TaskHandler:
    task_handler = TaskHandler(
        workers=[
            Worker(
                task_definition_name='rotate_image',
                execute_function=rotate_image,
                poll_interval=0.01,

            ),
            Worker(
                task_definition_name='blur_image',
                execute_function=blur_image,
                poll_interval=0.01,

            ),
            Worker(
                task_definition_name='flip_image',
                execute_function=flip_image,
                poll_interval=0.01,

            )
        ],
        configuration=get_configuration()
    )
    task_handler.start_processes()
    print('started all workers')
    return task_handler


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


if __name__ == '__main__':
    main()
