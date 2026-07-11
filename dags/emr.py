# Airflow DAG

from datetime import datetime

from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator, EmrAddStepsOperator, EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor, EmrStepSensor

AWS_CONN_ID="aws_default"

JOB_FLOW_OVERRIDES={
"Name":"data-engineering-emr",
"ReleaseLabel":"emr-spark-8.0.0",
"Applications":[{"Name":"Spark"}],
"LogUri":"s3://data-engineering7/logs/",
"Instances":{
"InstanceGroups":[
{"Name":"Primary","Market":"ON_DEMAND","InstanceRole":"MASTER","InstanceType":"m5.xlarge","InstanceCount":1},
{"Name":"Core","Market":"ON_DEMAND","InstanceRole":"CORE","InstanceType":"m5.xlarge","InstanceCount":2}],
"Ec2KeyName":"new1",
"Ec2SubnetId":"subnet-077c1a1e95f48149d",
"KeepJobFlowAliveWhenNoSteps":True,
"TerminationProtected":False},
"JobFlowRole":"AmazonEMR-InstanceProfile-20260709T110917",
"ServiceRole":"AmazonEMR-ServiceRole-20260709T110935",
"VisibleToAllUsers":True}

SPARK_STEPS=[{"Name":"Run s3.py","ActionOnFailure":"CONTINUE","HadoopJarStep":{"Jar":"command-runner.jar","Args":["spark-submit","s3://data-engineering7/dags/s3.py"]}}]

with DAG(dag_id="emr_s3_pipeline",start_date=datetime(2026,7,9),schedule=None,catchup=False) as dag:
    create_cluster=EmrCreateJobFlowOperator(task_id="create_cluster",job_flow_overrides=JOB_FLOW_OVERRIDES,aws_conn_id=AWS_CONN_ID)
    wait_for_cluster=EmrJobFlowSensor(task_id="wait_for_cluster",job_flow_id=create_cluster.output,target_states=["WAITING","RUNNING"],aws_conn_id=AWS_CONN_ID)
    add_steps=EmrAddStepsOperator(task_id="add_steps",job_flow_id=create_cluster.output,steps=SPARK_STEPS,aws_conn_id=AWS_CONN_ID)
    watch_step=EmrStepSensor(task_id="watch_step",job_flow_id=create_cluster.output,step_id="{{ task_instance.xcom_pull(task_ids='add_steps')[0] }}",aws_conn_id=AWS_CONN_ID)
    terminate_cluster=EmrTerminateJobFlowOperator(task_id="terminate_cluster",job_flow_id=create_cluster.output,aws_conn_id=AWS_CONN_ID,trigger_rule="all_done")
    create_cluster>>wait_for_cluster>>add_steps>>watch_step>>terminate_cluster
