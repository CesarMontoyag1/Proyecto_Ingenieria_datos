import boto3

def get_ssm_parameter(name: str, region: str = "us-east-1") -> str:
    """Lee un parámetro publicado por los stacks de infrastructure/ (storage, streaming)."""
    ssm = boto3.client("ssm", region_name=region)
    response = ssm.get_parameter(Name=name)
    return response["Parameter"]["Value"]


def get_raw_bucket_name(deploy_app: str = "nexasecure", stage: str = "dev") -> str:
    return get_ssm_parameter(f"/{deploy_app}/{stage}/storage/RawAwid3BucketName")


def get_stream_name(deploy_app: str = "nexasecure", stage: str = "dev") -> str:
    try:
        return get_ssm_parameter(f"/{deploy_app}/{stage}/streaming/Awid3StreamName")
    except Exception:
        return get_ssm_parameter(f"/{deploy_app}/{stage}/stream/Awid3StreamName")
