import boto3
REGION = 'eu-central-1'

ec2_ressource = boto3.resource('ec2', region_name=REGION)
ec2_client = boto3.client('ec2', region_name=REGION)

def lambda_handler(event, context):
    
    instances = ec2_ressource.instances.filter(Filters=[
        {'Name': 'instance-state-name', 'Values': ['running'], 
         'Name': 'tag:Name', 'Values': ['StockSentimentAnalyser']}
        ])
    print([ec2_client.terminate_instances(InstanceIds=[instance.id]) for instance in instances])

