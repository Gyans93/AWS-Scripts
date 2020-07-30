import boto3
from datetime import datetime
from botocore.client import Config
from botocore.exceptions import ClientError
from dateutil.tz import tzutc
import json

profiles = ['sandbox-access']
config = Config(region_name='us-east-1')
session = boto3.Session(profile_name=profiles[0])

ec2_client = session.client('ec2', config=config)
emr_client = session.client('emr', config=config)
s3_client = session.resource('s3', config=config)
cloudtrail_client = session.client('cloudtrail', config=config)
paginator = cloudtrail_client.get_paginator('lookup_events')

# Fetching the configuration json file from S3
# The bucket name and file name will be taken from the arguments
content_object = s3_client.Object('testcf-lambda-functions',
                                  'test-cluster-config.json')
file_content = content_object.get()['Body'].read().decode('utf-8')
s3_config = json.loads(file_content)

print("s3-config", s3_config)


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
        InstanceTypes=["t3a.nano", "t3a.small", "t2.micro"],
        StartTime=datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        ProductDescriptions=['Linux/UNIX'])
    return find_suitable_instances(response)


# Not needed right now
def get_instance_groups():
    return


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
                "InstanceType": "t3a.small",
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
            "InstanceType": "t3a.small",
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
        bidPrice = str(round(float(instance['SpotPrice']) * float(1.1), 4))
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


# Forming required bootstrap actions as per the s3 configuration file
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


# Creating cluster with all the above configurations
cluster = emr_client.run_job_flow(
    Name=s3_config['Name'],
    LogUri=s3_config['logs_s3_url'],
    ServiceRole=s3_config.setdefault('service-role-arn', ''),
    JobFlowRole='EMRCreateTestRole',
    ScaleDownBehavior="TERMINATE_AT_TASK_COMPLETION",
    # Ec2SubnetIds=s3_config.setdefault('Ec2SubnetIds', []),
    Instances={
        # "InstanceGroups": get_instance_groups(),
        "InstanceFleets": form_best_instance_fleet(),
    },
    Steps=get_steps(),
    BootstrapActions=get_BootstrapActions(),
    Applications=[
        {
            'Name': 'Ganglia',
        },
        {
            'Name': 'Spark',
        },
    ],
    Configurations=[{
        "Classification": "spark-defaults",
        "Properties": {
            "spark.executor.memory":
            "15g",
            "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version":
            "1",
            "spark.driver.memory":
            "15g",
            "spark.driver.maxResultSize":
            "7g",
            "spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored.emr_internal_use_only.EmrFileSystem":
            "true",
            "spark.scheduler.listenerbus.eventqueue.capacity":
            "100000",
            "spark.decommissioning.timeout.threshold":
            "20",
            "spark.network.timeout":
            "120",
            "spark.executor.cores":
            "5",
            "spark.sql.shuffle.partitions":
            "512",
            "spark.default.parallelism":
            "512",
            "spark.worker.cleanup.enabled":
            "true",
            "spark.executor.memoryOverhead":
            "7g",
            "spark.sql.parquet.fs.optimized.committer.optimization-enabled":
            "false",
            "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version.emr_internal_use_only.EmrFileSystem":
            "1",
            "spark.executor.instances":
            "1000",
            "spark.dynamicAllocation.enabled":
            "false",
            "spark.executer.heartbeatInterval":
            "20s",
            "spark.files.fetchFailure.unRegisterOutputOnHost":
            "True"
        }
    }, {
        "Classification": "emrfs-site",
        "Properties": {
            "fs.s3.consistent.retryPeriodSeconds": "10",
            "fs.s3.consistent": "false",
            "fs.s3.serverSideEncryption.kms.keyId":
            "arn:aws:kms:us-east-1:257332437798:key/cac67632-ae39-4daa-a50d-e99d50036034",
            "fs.s3.enableServerSideEncryption": "true",
            "fs.s3.consistent.retryCount": "5",
            "fs.s3.consistent.metadata.tableName": "MDAEmrFSMetadata"
        }
    }],
    Tags=get_tags(),
    # ManagedScalingPolicy={},
)
# print(cluster)
print('cluster created :', cluster['JobFlowId'])





