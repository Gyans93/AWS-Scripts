import boto3
import json
import sys
from datetime import datetime
today = datetime.now()

connection = boto3.client('emr', region_name='us-east-1')
ec2_client = boto3.client('ec2', region_name='us-east-1')
s3Client = boto3.resource('s3', region_name='us-east-1')
cloudtrail_client = boto3.client('cloudtrail', region_name='us-east-1')
paginator = cloudtrail_client.get_paginator('lookup_events')

if len(sys.argv) < 2:
    print("Please provide a S3 bucket name and the file name for configuration")

content_object = s3Client.Object(sys.argv[1], sys.argv[2])
file_content = content_object.get()['Body'].read().decode('utf-8')
s3_config = json.loads(file_content)


def get_BootstrapActions():
    actionsList = s3_config['bootstrap-actions']
    actions = []
    for item in actionsList:
        actions.append({
            "Name": item['Name'],
            "ScriptBootstrapAction": {
                "Path": item['Path'],
                "Args": item.setdefault('Args', [])
            }
        })
    return actions

# print(get_BootstrapActions())

# Forming steps config as per the s3 configuration file
def get_steps():
    steps = s3_config['Steps']
    finalSteps = []
    for item in steps:
        finalSteps.append({
            "Name": item['Name'],
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": item['Jar'],
                "Args": item['Args']
            }
        })
    return finalSteps

# Extracting suitable spot instances from the response
def find_suitable_instances(response):
    pricingDict = {}  #stores instances along with their price
    if response['SpotPriceHistory']:
        for instance in response['SpotPriceHistory']:
            if instance['InstanceType'] not in pricingDict:
                pricingDict[instance['InstanceType']] = instance

    # Fetch cloudtrail events to count the number of times an instance failed using paginator
    page_iterator = paginator.paginate(
        LookupAttributes=[{
            'AttributeKey': 'EventName',
            'AttributeValue': 'RunInstances'
        }],
        StartTime=today - timedelta(days=7),
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
                if count > 100:
                    del pricingDict[instanceType]

    instanceList = list(pricingDict.values())
    instanceList.sort(key=lambda x: x['SpotPrice'])

    # Selecting first n cheapest spot instances from the list based on #tasknodes required
    return instanceList[0:s3_config['instance-config']['number-of-taskNodes']]


def fetch_current_spotPrices():
    response = ec2_client.describe_spot_price_history(
        # More instance types can be added here
        InstanceTypes=["m1.medium", "m1.small"],
        StartTime=today.strftime("%Y-%m-%dT%H:%M:%S"),
        ProductDescriptions=['Linux/UNIX'])
    return find_suitable_instances(response)


def form_best_instance_fleet():
    # Fetching right set of spot instances
    cheapInstances = fetch_current_spotPrices()
    instance_config = s3_config['instance-config']

    # Configuration for master node
    master_node = {
        "Name":
        "Master - 1",
        "InstanceFleetType":
        "MASTER",
        "TargetOnDemandCapacity":
        1,
        "InstanceTypeConfigs": [
            {
                "InstanceType": "m4.large",
                "EbsConfiguration": {
                    "EbsBlockDeviceConfigs": [{
                        "VolumeSpecification": {
                            "SizeInGB": instance_config['master-SizeInGB'],
                            "VolumeType": "gp2"
                        },
                        "VolumesPerInstance": 1
                    }]
                },
            },
        ],
    }

    # Configuration for core nodes
    core_node = {
        "Name":
        "Core - 2",
        "InstanceFleetType":
        "CORE",
        "TargetOnDemandCapacity":
        1,
        "InstanceTypeConfigs": [{
            "InstanceType": "m4.large",
            "EbsConfiguration": {
                "EbsBlockDeviceConfigs": [{
                    "VolumeSpecification": {
                        "SizeInGB": instance_config['core-SizeInGB'],
                        "VolumeType": "gp2"
                    },
                    "VolumesPerInstance": 1
                }]
            },
        }],
    }

    # Configuration of task nodes
    totalCapacityInGB = instance_config['task-total-SizeInGB']
    InstanceTypeConfigs = []

    # Creating bunch of task node instance-type configuration
    for instance in cheapInstances:
        # Prices are set to bid 10% higher than the actual rate
        bidPrice = str(round(float(instance['SpotPrice']) * float(1.1), 3))
        taskConfig = {
            "BidPrice": bidPrice,
            "WeightedCapacity": 1,
            "EbsConfiguration": {
                "EbsBlockDeviceConfigs": [{
                    "VolumeSpecification": {
                        "SizeInGB":
                        # Dividing total task nodes capacity equally among instances
                        int(totalCapacityInGB /
                            instance_config['number-of-taskNodes']),
                        "VolumeType":
                        "gp2"
                    },
                    "VolumesPerInstance": 1
                }]
            },
            "InstanceType": instance['InstanceType']
        }
        InstanceTypeConfigs.append(taskConfig)

    task_node = {
        "Name": "Task - 3",
        "InstanceFleetType": "TASK",
        "TargetOnDemandCapacity": 0,
        "TargetSpotCapacity": 1,
        "LaunchSpecifications": {
            "SpotSpecification": {
                "TimeoutDurationMinutes": 15,
                "TimeoutAction": "SWITCH_TO_ON_DEMAND"
            }
        },
        "InstanceTypeConfigs": InstanceTypeConfigs
    }
    return [master_node, core_node, task_node]


print(form_best_instance_fleet())

# Setting tag list from s3 configuration file in addition to the below default tags
def get_tags():
    tags = {
        "OSType": "AmazonLinux",
        "ServiceType": "App",
        "BusinessImpact": "Low",
        "BusinessUnit": "FrontOffice",
        "Project": "MemberDataAggregation",
        "Transient": "true",
        "AppName": "MemberDataAggregation",
    }
    tags.update(s3_config['tags'])
    tagList = []
    for key in tags:
        tagList.append({"Key": key, "Value": tags[key]})
    return tagList

cluster_id = connection.run_job_flow(
    Name=s3_config['Name'],
    ReleaseLabel='emr-5.26.0',
    # LogUri=s3_config['logs_s3_url'],
    Applications=[
	{
            'Name': 'Ganglia',
        },
        {
            'Name': 'Spark'
        }
    ],
    Instances={
	"InstanceFleets": form_best_instance_fleet(),
        'Ec2KeyName':
        'emt-test-new-putty',
        'KeepJobFlowAliveWhenNoSteps':
        True,
        'TerminationProtected':
        False,
        "Ec2SubnetIds": s3_config.setdefault('Ec2SubnetIds', []),
    },
    VisibleToAllUsers=True,
    BootstrapActions=get_BootstrapActions(),
    ServiceRole=s3_config.setdefault('service-role-arn', ""),
    JobFlowRole=s3_config['JobFlowRole'],
    # Steps=get_steps(),
    Tags=get_tags(),
)

print('cluster created with the step...', cluster_id['JobFlowId'])


