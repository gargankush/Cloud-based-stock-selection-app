import boto3

def lambda_handler(event, context):
    emr = boto3.client("emr")        
    cluster_id = emr.run_job_flow(
        Name='Project',
        StepConcurrencyLevel= 3,
        ServiceRole='EMR_DefaultRole',
        JobFlowRole='EMR_EC2_DefaultRole',
        VisibleToAllUsers=True,
        LogUri='s3://aws-logs-276025053653-us-east-1/elasticmapreduce/',
        ReleaseLabel='emr-5.31.0',
        Instances={
            'InstanceGroups': [
                {
                    'Name': 'Master',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 1,
                },
                {
                    'Name': 'Core',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 1,
                }
            ],
            'KeepJobFlowAliveWhenNoSteps': True,
            'TerminationProtected': False
        },
        Applications=[{
            'Name': 'Spark'
        },
        {
            'Name': 'Hadoop'
        },
        {
            'Name': 'Livy'
        }
        ],
        BootstrapActions=[
    {
        'Name': 'Boostrap EMR',
        'ScriptBootstrapAction': {
            'Path': 's3://web-app-project/bootstrap.sh'
        }
    }
])
    response = {}
    response["cluster_id"] = cluster_id["JobFlowId"]
    return response
