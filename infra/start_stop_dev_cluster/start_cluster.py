import json
import boto3
import logging
# import requests

# def publish_dev_ip(new_dev_ip, session) -> None:
#     """
#     Publishes the Airflow Dev IP to Microsoft Teams.
#     """
#     client = session.client("secretsmanager")
#     secret_name = 'teams_webhook_secret'
#     credentials = client.get_secret_value(
#         SecretId=secret_name
#     )
#     stack_teams_webhook_secret = credentials.get('stack_teams_webhook_secret')
#     alloha_teams_webhook_secret = credentials.get('alloha_teams_webhook_secret')
        
#     payload = {
#         "@type": "MessageCard",
#         "@context": "http://schema.org/extensions",
#         "title": "Dev Cluster Started",
#         "summary": f"Dev Cluster IP",
#         "themeColor": "9eff00",
#         "sections": [
#             {
#                 "activityTitle": "",
#                 "activitySubtitle": "",
#                 "facts": [
#                     {"name": "New IP:", "value": f'http://{new_dev_ip}:8080/'},
#                 ]
#             }
#         ]
#     }

    # headers = {"content-type": "application/json"}
    # requests.post(stack_teams_webhook_secret, json=payload, headers=headers)
    # requests.post(alloha_teams_webhook_secret, json=payload, headers=headers)
    
def start_cluster() -> None:
    session = boto3.Session()
    client = session.client('ec2')

    instance_id = 'i-041a641edd66eedc8'

    response = client.start_instances(
        InstanceIds=[
            instance_id,
        ],
    )
    
    state = response['StartingInstances'][0]['CurrentState']['Name']
    if state == 'running':
        logging.info(f'Instance {instance_id} already running')
    elif state == 'pending':
        logging.info(f'Instance {instance_id} started successfully')
    else:
        logging.info(f'Instance {instance_id} could not be started')
        return
        
    # ec2 = session.resource('ec2')
    # instance = ec2.Instance(instance_id)
    # instance.wait_until_running()
    # instance_ip = instance.public_ip_address
    
    # if 'instance_ip' in locals():
    #     publish_dev_ip(instance_ip, session)
    #     logging.info(f"Teams notification sent with new IP: {instance_ip}")
    # else:
    #     logging.info(f"Instance IP could not be found")
    
    return

def lambda_handler(event, context):
    start_cluster()
    return {
        'statusCode': 200,
        'body': json.dumps('Success!')
    }
