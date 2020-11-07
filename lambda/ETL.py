import boto3

def lambda_handler(message, context):
    
    emr = boto3.client("emr", region_name="us-east-1")
    
    # we retrive the cluster_id that was passed by the lambda function
    cluster_id = message["cluster_id"]
    
    step1 = {'Name': 'twitter_etl',
           'ActionOnFailure': 'CONTINUE',
           'HadoopJarStep': {
               'Jar': 'command-runner.jar',
               'Args': ["spark-submit", "--deploy-mode", "cluster", "s3://web-app-project/spark-jobs/twitter_data_etl.py"]
           }
        }
    
    step2 = {'Name': 'price_etl',
           'ActionOnFailure': 'CONTINUE',
           'HadoopJarStep': {
               'Jar': 'command-runner.jar',
               'Args': ["spark-submit", "--deploy-mode", "cluster", "s3://web-app-project/spark-jobs/price_data_etl.py"]
           }
        }
    # add concurrent steps
    action = emr.add_job_flow_steps(JobFlowId=cluster_id, Steps=[step1, step2])
    response = {}
    response["step_id"] = action["StepIds"][0]
    response["cluster_id"] = cluster_id
    return response
