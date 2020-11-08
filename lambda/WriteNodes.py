import boto3

def lambda_handler(message, context):
    
    emr = boto3.client("emr", region_name="us-east-1")
    
    # we retrieve the cluster_id that was passed by the lambda function
    cluster_id = message["cluster_id"]
    
    step = {'Name': 'write_nodes',
           'ActionOnFailure': 'CONTINUE',
           'HadoopJarStep': {
               'Jar': 'command-runner.jar',
               'Args': ["spark-submit", "--deploy-mode", "cluster", "s3://web-app-project/spark-jobs/write_nodes.py"]
           }
        }
    
    action = emr.add_job_flow_steps(JobFlowId=cluster_id, Steps=[step])
    response = {}
    response["step_id"] = action["StepIds"][0]
    response["cluster_id"] = cluster_id
    return response
