import boto3

def lambda_handler(message, context):
    
    emr = boto3.client("emr", region_name="us-east-1")
    
    # we retrieve the cluster_id and step_id that were passed by the lambda function
    cluster_id = message["cluster_id"]
    step_id = message["step_id"]
    
    # We retrieve the steps from the cluster
    steps = emr.list_steps(ClusterId=cluster_id)["Steps"]
    
    # We identify the one we need with its id
    step = [step for step in steps if step["Id"] == step_id]
    
    # We retrieve its state
    state = step[0]["Status"]["State"]
    
    response = {}
    response["state"] = state
    response["step_id"] = step_id
    response["cluster_id"] = cluster_id
    return response
