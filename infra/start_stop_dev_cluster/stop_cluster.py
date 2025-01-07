import json
import boto3
import logging

def stop_cluster():
    session = boto3.Session()
    client = session.client('ec2')

    instance_id = 'i-041a641edd66eedc8'

    client.stop_instances(
        InstanceIds=[
            instance_id,
        ],
    )
    
    logging.info(f'Instance {instance_id} stopped successfully')
    
def lambda_handler(event, context):
    stop_cluster()
    return {
        'statusCode': 200,
        'body': json.dumps('Success!')
    }