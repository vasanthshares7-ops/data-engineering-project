from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator, EmrAddStepsOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import pendulum

local_tz = pendulum.timezone("Asia/Kolkata")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(0),
    'timezone': local_tz,
}

dag = DAG(
    'FINALDAG',
    default_args=default_args,
    description='DAG to create an EMR cluster and submit Spark steps',
    schedule_interval='3 9 * * *',  # Daily at 03:15 UTC
    tags=['emr', 'spark'],
    catchup=False,
)

JOB_FLOW_OVERRIDES = {
    "Name": "prod-cluster",
    "LogUri": "s3://aws-logs-886256265598-ap-south-1/elasticmapreduce",
    "ReleaseLabel": "emr-7.1.0",
    "ServiceRole": "arn:aws:iam::886256265598:role/service-role/AmazonEMR-ServiceRole-20250522T164731",
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Primary",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
                "EbsConfiguration": {
                    "EbsBlockDeviceConfigs": [
                        {
                            "VolumeSpecification": {
                                "VolumeType": "gp2",
                                "SizeInGB": 32,
                            },
                            "VolumesPerInstance": 2,
                        }
                    ]
                }
            }
        ],
        "Ec2KeyName": "45kp",
        "Ec2SubnetId": "subnet-04534c8c8e9f84922",
        "EmrManagedMasterSecurityGroup": "sg-0cce408da49637ae8",
        "EmrManagedSlaveSecurityGroup": "sg-02c7b56b87af62f26",
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
    },
    "Applications": [
        {"Name": "Hadoop"},
        {"Name": "Spark"},
    ],
    "VisibleToAllUsers": True,
    "JobFlowRole": "AmazonEMR-InstanceProfile-20250522T164714",
    "Tags": [
        {
            "Key": "for-use-with-amazon-emr-managed-policies",
            "Value": "true"
        }
    ],
    "ScaleDownBehavior": "TERMINATE_AT_TASK_COMPLETION",
    "AutoTerminationPolicy": {
        "IdleTimeout": 60
    }
}

SPARK_STEPS = [
    {
        'Name': 's3Job',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode', 'client',
                '--master', 'local[*]',
                's3://zeyos44/pyfiles/s3.py'
            ]
        }
    },
    {
        'Name': 'SnowJob',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode', 'client',
                '--packages', 'net.snowflake:spark-snowflake_2.12:3.1.1',
                '--master', 'local[*]',
                's3://zeyos44/pyfiles/snow.py'
            ]
        }
    },
    {
        'Name': 'MasterJob',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode', 'client',
                '--packages', 'net.snowflake:spark-snowflake_2.12:3.1.1',
                '--master', 'local[*]',
                's3://zeyos44/pyfiles/master.py'
            ]
        }
    }
]

# Create EMR Cluster
create_emr_cluster = EmrCreateJobFlowOperator(
    task_id='create_emr_cluster',
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id='aws_default',
    region_name='ap-south-1',
    dag=dag,
)

# Add Spark Steps
add_spark_steps = EmrAddStepsOperator(
    task_id='add_spark_steps',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id='aws_default',
    steps=SPARK_STEPS,
    dag=dag,
)

create_emr_cluster >> add_spark_steps
