import boto3

connection = boto3.client('emr', region_name='us-east-1')

cluster_id = connection.run_job_flow(
    Name='test_emr_job',
    ReleaseLabel='emr-5.26.0',
    Applications=[
        {
            'Name': 'Spark'
        },
    ],
    Instances={
        "InstanceFleets": [{
            "Name":
            "Master - 1",
            "InstanceFleetType":
            "MASTER",
            "TargetOnDemandCapacity":
            1,
            "InstanceTypeConfigs": [
                {
                    "InstanceType": "m5.xlarge",
                    "EbsConfiguration": {
                        "EbsBlockDeviceConfigs": [{
                            "VolumeSpecification": {
                                "SizeInGB": 32,
                                "VolumeType": "gp2"
                            },
                            "VolumesPerInstance": 1
                        }]
                    },
                },
            ],
        }],
        'Ec2KeyName':
        'emt-test-new-putty',
        'KeepJobFlowAliveWhenNoSteps':
        True,
        'TerminationProtected':
        False,
        "Ec2SubnetIds": ["subnet-0dd044f0aeb61f951"],
    },
    VisibleToAllUsers=True,
    ServiceRole='arn:aws:iam::030743829926:role/EMRCreateTestRoleForEC2',
    JobFlowRole='arn:aws:iam::030743829926:instance-profile/EMRCreateTestRoleForEC2',
    LogUri="s3://testcf-lambda-functions/emrlogs/",
    Steps=[{
            "Name":"pi-app",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar":"command-runner.jar",
                "Args":["spark-submit","--deploy-mode","cluster","s3://testcf-lambda-functions/pi.py"]
            }
        }],
    Tags=[
        {
            'Key': 'Owner',
            'Value': "gprakash@bjs.com",
        }
    ],
)

print('cluster created with the step...', cluster_id['JobFlowId'])

