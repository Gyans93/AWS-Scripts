import boto3
import json
from datetime import datetime

connection = boto3.client('emr', region_name='us-east-1')
ec2_client = boto3.client('ec2', region_name='us-east-1')
s3Client = boto3.resource('s3', region_name='us-east-1')
cloudtrail_client = boto3.client('cloudtrail', region_name='us-east-1')
paginator = cloudtrail_client.get_paginator('lookup_events')

content_object = s3Client.Object('testcf-lambda-functions',
                                  'test-cluster-config.json')
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

print(get_BootstrapActions())

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
        EndTime=datetime.now(),
        StartTime=datetime(2020, 7, 19),
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
        StartTime=datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
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

    # Configuration of task nodes
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


cluster_id = connection.run_job_flow(
    Name='test_emr_job_boto3',
    ReleaseLabel='emr-5.26.0',
    LogUri=s3_config['logs_s3_url'],
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
        "Ec2SubnetIds": ["subnet-0dd044f0aeb61f951"],
    },
    VisibleToAllUsers=True,
    BootstrapActions=get_BootstrapActions(),
    ServiceRole='arn:aws:iam::030743829926:role/EMRCreateTestRole',
    JobFlowRole='EMRCreateTestRole',
    Steps=[
        {
            'Name': 'file-copy-step',
                    'ActionOnFailure': 'CONTINUE',
                    'HadoopJarStep': {
                        'Jar': 's3://kula-emr-test/jars/CopyFilesS3-1.0-SNAPSHOT-jar-with-dependencies.jar',
                        'Args': ['test.xml', 'kula-emr-test', 'kula-emr-test-2']
                    }
        }
    ],
    Tags=[
        {
            'Key': 'tag_name_1',
            'Value': 'tab_value_1',
        },
        {
            'Key': 'tag_name_2',
            'Value': 'tag_value_2',
        },
    ],
)

print('cluster created with the step...', cluster_id['JobFlowId'])

