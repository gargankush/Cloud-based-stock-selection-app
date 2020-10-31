import boto3

def lambda_handler(message, context):
    
    emr = boto3.client("emr", region_name="us-east-1")
    
    # we retrieve the cluster_id that was passed by the previous lambda function
    cluster_id = message["cluster_id"]
    
    response = emr.terminate_job_flows(JobFlowIds=[cluster_id])
    return f"Terminated cluster {cluster_id}"
