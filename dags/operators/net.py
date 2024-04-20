import os
import logging
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

def download_scihub(productIdentifier, storageFolder, outFolder, task_id, queue='default', priority_weight=0):
    return BashOperator(
        task_id=task_id,
        bash_command=f" mkdir {storageFolder}/{productIdentifier} \
            && cd {storageFolder}/{productIdentifier} \
            && wget --content-disposition --continue \
                --user=\"{os.environ.get('OPNHUB_USER')}\" \
                --password=\"{os.environ.get('OPNHUB_PASS')}\" \
                \"https://scihub.copernicus.eu/dhus/odata/v1/Products('{productIdentifier}')/\$value\" \
            && unzip *.zip \
            && mv */ ../{outFolder}/",
        queue=queue,
        priority_weight=priority_weight,
        weight_rule='absolute'
    )   

def process_aws_download_folder(bucket_name, prefix, local_dir, requesterPays):
    import boto3

    client = boto3.Session(
        aws_access_key_id=os.environ.get('S3_ACCESS_KEY_ID'), 
        aws_secret_access_key=os.environ.get('S3_SECRET_ACCESS_KEY'),
        ).client('s3')
    
    default_kwargs = {
        "Bucket": bucket_name,
        "Prefix": prefix,
        'RequestPayer': 'requester' if requesterPays else None
    }
    response = client.list_objects_v2(**default_kwargs)

    contents = response.get("Contents")
    for result in contents:
        key = result.get("Key")
        target = os.path.join(local_dir, key.replace(prefix, ""))
        if not os.path.exists(target):
            os.makedirs(os.path.dirname(target), exist_ok=True)
        if key[-1] == '/':
            continue
        client.download_file(bucket_name, key, target, ExtraArgs={'RequestPayer': 'requester'})

def process_aws_download_file(bucket, prefix, file, local_storage, requesterPays):
    import boto3

    client = boto3.Session(
        aws_access_key_id=os.environ.get('S3_ACCESS_KEY_ID'), 
        aws_secret_access_key=os.environ.get('S3_SECRET_ACCESS_KEY'),
        ).client('s3')
    
    local_target = os.path.join(local_storage, file)
    object_name = os.path.join(prefix, file)

    if not os.path.isdir(local_storage):
        os.makedirs(local_storage)

    client.download_file(bucket, object_name, local_target)

def download_aws(bucket, prefix, file, local_storage, task_id, queue, priority_weight, requesterPays = True):
    return PythonOperator(
        task_id=task_id,
        python_callable=process_aws_download_file,
        op_args=[bucket, prefix, file, local_storage, requesterPays],
        queue=queue,
        priority_weight=priority_weight,
    )


def unzip(zip_file_path, task_id, queue, priority_weight):
    out_folder = os.path.dirname(zip_file_path)
    return BashOperator(
        task_id=task_id,
        bash_command=f"cd {out_folder} && unzip -o {zip_file_path} -d . ",
        queue=queue,
        priority_weight=priority_weight,
    )

if __name__ == '__main__':
    with open(os.path.dirname(__file__) + '/../../.env') as f:
        for line in f:
            if line.startswith('#') or not line.strip():
                continue
            key, value = line.strip().split('=', 1)
            os.putenv(key, value)
    process_aws_download(
        'earth-in-the-cloud', 
        'test-products/sentinel-2/', 
        'S2B_MSIL2A_20210806T100559_N0301_R022_T32TMK_20210806T150037.zip',
        '/tmp/p1', 
        False)

