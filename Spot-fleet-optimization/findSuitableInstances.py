import boto3
from datetime import datetime, timedelta
from botocore.client import Config
from botocore.exceptions import ClientError
from dateutil.tz import tzutc
import json
import csv

connection = boto3.client('emr', region_name='us-east-1')
ec2_client = boto3.client('ec2', region_name='us-east-1')
#s3Client = boto3.resource('s3', region_name='us-east-1')
cloudtrail_client = boto3.client('cloudtrail', region_name='us-east-1')

paginator = cloudtrail_client.get_paginator('lookup_events')

instanceTypes = [
    "m4.large", "m4.xlarge", "m4.2xlarge", "m4.4xlarge", "m4.10xlarge",
    "m5. m4.16xlarge", "m5.xlarge", "m5.2xlarge", "m5.4xlarge", "m5.8xlarge",
    "m5.12xlarge", "m5.16xlarge", "m5.24xlarge", "m5a.xlarge", "m5a.2xlarge",
    "m5a.4xlarge", "m5a.8xlarge", "m5a.12xlarge", "m5a.16xlarge",
    "m5a.24xlarge", "m5d.xlarge", " m5d.2xlarge", "m5d.4xlarge", "m5d.8xlarge",
    "m5d.12xlarge", "m5d.16xlarge", "m5d.24xlarge", "c4.large", "c4.xlarge",
    "c4.2xlarge", "c4.4xlarge", "c4.8xlarge", "c5.xlarge³", "c5.2xlarge",
    "c5.4xlarge", "c5.9xlarge", "c5.12xlarge", "c5.18xlarge", "c5.24xlarge",
    "c5d.xlarge", "c5d.2xlarge", "c5d.4xlarge", "c5d.9xlarge", "c5d.12xlarge",
    "c5d.18xlarge", "c5d.24xlarge", "c5n.xlarge", "c5n.2xlarge", "c5n.4xlarge",
    "c5n.9xlarge", "c5n.18xlarge", "cc2.8xlarge", "z1d.xlarge", "z1d.2xlarge",
    "z1d.3xlarge", "z1d.6xlarge", "z1d.12xlarge", "r4.xlarge", "r4.2xlarge",
    "r4.4xlarge", "r4.8xlarge", "r4.16xlarge", "r5.xlarge", "r5.2xlarge",
    "r5.4xlarge", "r5.8xlarge", "r5.12xlarge", "r5.16xlarge", "r5.24xlarge",
    "r5a.xlarge", "r5a.2xlarge", "r5a.4xlarge", "r5a.8xlarge", "r5a.12xlarge",
    "r5a.16xlarge", "r5a.24xlarge", "r5d.xlarge", "r5d.2xlarge", "r5d.4xlarge",
    "r5d.8xlarge", "r5d.12xlarge", "r5d.16xlarge", "r5d.24xlarge",
    "h1.2xlarge", "h1.4xlarge", "h1.8xlarge", "h1.16xlarge"
]

today = datetime.now()


# Extracting suitable spot instances from the response
def find_suitable_instances(response):
    pricingDict = {}  #stores instances along with their price
    allowed_subnets = ["us-east-1c", "us-east-1d", "us-east-1f"]
    if response['SpotPriceHistory']:
        for instance in response['SpotPriceHistory']:
            if instance['AvailabilityZone'] in allowed_subnets and instance[
                    'InstanceType'] not in pricingDict:
                pricingDict[instance['InstanceType']] = instance
    # print(pricingDict)

    # Fetch cloudtrail events to count the number of times an instance failed using paginator

    page_iterator = paginator.paginate(
        LookupAttributes=[{
            'AttributeKey': 'EventName',
            'AttributeValue': 'RunInstances'
        }],
        StartTime=today - timedelta(days=6),
        EndTime=today,
    )
    for page in page_iterator:
        for item in page["Events"]:
            event = json.loads(item['CloudTrailEvent'])
            instanceType = event['requestParameters']['instanceType']
            if instanceType in pricingDict and event.setdefault(
                    'errorCode', '') == "Server.InsufficientInstanceCapacity":
                count = pricingDict[instanceType].setdefault('failCount',
                                                             0) + 1
                pricingDict[instanceType]['failCount'] = count
                # Removing instances which failed more than 100 times in past week
                print("Following spot instances failed more than 100 times in the past week:")
                if count > 100:
                    print(instanceType)
                    del pricingDict[instanceType]

    instanceList = list(pricingDict.values())
    instanceList.sort(key=lambda x: x['SpotPrice'])

    return instanceList


def fetch_current_spotPrices():
    response = ec2_client.describe_spot_price_history(
        # More instance types can be added here
        InstanceTypes=instanceTypes,
        StartTime=datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        ProductDescriptions=['Linux/UNIX'])
    return find_suitable_instances(response)


spotPriceList = fetch_current_spotPrices()
# print(spotPriceList)


def list_existing_clusters():
    res = cloudtrail_client.lookup_events(
        LookupAttributes=[{
            'AttributeKey': 'EventName',
            'AttributeValue': 'RunJobFlow'
        }],
        StartTime=today - timedelta(days=7),
        EndTime=today,
    )
    existingInstances = {}
    for event in res['Events']:
        eventJson = json.loads(event['CloudTrailEvent'])
        reqParams = eventJson.setdefault('requestParameters', {})
        if reqParams is None:
            return existingInstances
        instances = reqParams.setdefault('instances', {})
        instanceFleets = instances.setdefault('instanceFleets', [])
        task_node = next((node for node in instanceFleets
                          if node['instanceFleetType'] == "TASK"), None)
        # print(task_node)
        if task_node is None:
            return existingInstances
        instanceConfigs = task_node.setdefault('instanceTypeConfigs', [])
        for node in instanceConfigs:
            existingInstances.setdefault(node['instanceType'],
                                         set()).add(node['bidPrice'])
    return existingInstances


existingInstances = list_existing_clusters()

with open('out.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow([
        "Instance Type", "Spot Price", "Availability Zone", "OS",
        "Last Week Bid Prices"
    ])
    for item in spotPriceList:
        if float(item['SpotPrice']) <= 1.50:
            writer.writerow([
                item['InstanceType'], item['SpotPrice'],
                item['AvailabilityZone'], item['ProductDescription'],
                existingInstances.setdefault(item['InstanceType'], "N/A")
            ])
