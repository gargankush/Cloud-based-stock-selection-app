import boto3


def lambda_handler(message, context):
    
    emr = boto3.client("emr", region_name="us-east-1")
    
    cluster_id = message["cluster_id"]
    
    clusters = emr.list_clusters()
    
    # We check if the cluster is up
    state = [c["Status"]["State"] for c in clusters["Clusters"] if c["Id"] == cluster_id]
    response = {}
    response["cluster_id"] = cluster_id
    response["state"] = state[0]
    return response
