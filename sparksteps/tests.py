# -*- coding: utf-8 -*-
"""Test SparkSteps."""
import os

import shlex

import boto3
import moto

from sparksteps.steps import setup_steps, S3DistCp
from sparksteps.cluster import emr_config

TEST_BUCKET = 'sparksteps-test'
AWS_REGION_NAME = 'us-east-1'

resources_dir = os.path.join(os.path.dirname(__file__), '../examples')
CONF_FILE = os.path.join(resources_dir, 'cluster.json')
LIB_DIR = os.path.join(resources_dir, 'lib')
EPISODES_APP = os.path.join(resources_dir, 'episodes.py')
EPISODES_AVRO = os.path.join(resources_dir, 'episodes.avro')


@moto.mock_emr
def test_emr_config():
    config = emr_config('emr-4.6.0', master='m4.large', keep_alive=False,
                        slave='m4.2xlarge', num_core=1, num_task=1,
                        bid_price='0.1', conf_file=CONF_FILE)
    assert config == {'Instances': {'MasterInstanceType': 'm4.large',
                                    'TerminationProtected': True,
                                    'KeepJobFlowAliveWhenNoSteps': False,
                                    'SlaveInstanceType': 'm4.2xlarge',
                                    'InstanceGroups': [{'InstanceCount': 1,
                                                        'InstanceType': 'm4.large',
                                                        'Market': 'ON_DEMAND',
                                                        'Name': 'Master Node',
                                                        'InstanceRole': 'MASTER'},
                                                       {'InstanceCount': 1,
                                                        'InstanceType': 'm4.2xlarge',
                                                        'Market': 'ON_DEMAND',
                                                        'Name': 'Core Nodes',
                                                        'InstanceRole': 'CORE'},
                                                       {'BidPrice': '0.1',
                                                        'InstanceType': 'm4.2xlarge',
                                                        'InstanceRole': 'TASK',
                                                        'Name': 'Task Nodes',
                                                        'InstanceCount': 1,
                                                        'Market': 'SPOT'}],
                                    'InstanceCount': 1},
                      'Applications': [{'Name': 'Spark'}],
                      'Name': 'Test SparkSteps',
                      'JobFlowRole': 'EMR_EC2_DefaultRole',
                      'ReleaseLabel': 'emr-4.7.0', 'Configurations': [
            {'Classification': 'spark-defaults',
             'Properties': {'spark.executor.instances': '1',
                            'spark.dynamicAllocation.enabled': 'false'}}],
                      'VisibleToAllUsers': True,
                      'ServiceRole': 'EMR_DefaultRole'}

    client = boto3.client('emr', region_name=AWS_REGION_NAME)
    client.run_job_flow(**config)


@moto.mock_s3
def test_setup_steps():
    s3 = boto3.resource('s3', region_name=AWS_REGION_NAME)
    s3.create_bucket(Bucket=TEST_BUCKET)
    steps = (setup_steps(s3,
                         TEST_BUCKET,
                         EPISODES_APP,
                         submit_args="--jars /home/hadoop/lib/spark-avro_2.10-2.0.2-custom.jar".split(),
                         app_args="--input /home/hadoop/episodes.avro".split(),
                         uploads=[LIB_DIR, EPISODES_AVRO])
             )
    assert steps == [
        {'HadoopJarStep': {'Jar': 'command-runner.jar',
                           'Args': ['aws', 's3', 'cp',
                                    's3://sparksteps-test/sparksteps/sources/lib.zip',
                                    '/home/hadoop/']},
         'ActionOnFailure': 'CANCEL_AND_WAIT',
         'Name': 'Copy lib.zip'},
        {'HadoopJarStep': {'Jar': 'command-runner.jar',
                           'Args': ['unzip', '-o', '/home/hadoop/lib.zip',
                                    '-d', '/home/hadoop/lib']},
         'ActionOnFailure': 'CANCEL_AND_WAIT',
         'Name': 'Unzip lib.zip'},
        {'HadoopJarStep': {'Jar': 'command-runner.jar',
                           'Args': ['aws', 's3', 'cp',
                                    's3://sparksteps-test/sparksteps/sources/episodes.avro',
                                    '/home/hadoop/']},
         'ActionOnFailure': 'CANCEL_AND_WAIT',
         'Name': 'Copy episodes.avro'},
        {'HadoopJarStep': {'Jar': 'command-runner.jar',
                           'Args': ['aws', 's3', 'cp',
                                    's3://sparksteps-test/sparksteps/sources/episodes.py',
                                    '/home/hadoop/']},
         'ActionOnFailure': 'CANCEL_AND_WAIT', 'Name': 'Copy episodes.py'},
        {'HadoopJarStep': {'Jar': 'command-runner.jar',
                           'Args': ['spark-submit', '--jars',
                                    '/home/hadoop/lib/spark-avro_2.10-2.0.2-custom.jar',
                                    '/home/hadoop/episodes.py', '--input',
                                    '/home/hadoop/episodes.avro']},
         'ActionOnFailure': 'CANCEL_AND_WAIT',
         'Name': 'Run episodes.py'}]


def test_s3_dist_cp_step():
    splitted = shlex.split(
        "--s3Endpoint=s3.amazonaws.com --src=s3://mybucket/logs/j-3GYXXXXXX9IOJ/node/ --dest=hdfs:///output --srcPattern=.*[a-zA-Z,]+")
    assert S3DistCp(splitted).step == {'ActionOnFailure': 'CONTINUE',
                                       'HadoopJarStep': {'Args': ['s3-dist-cp',
                                                                  '--s3Endpoint=s3.amazonaws.com',
                                                                  '--src=s3://mybucket/logs/j-3GYXXXXXX9IOJ/node/',
                                                                  '--dest=hdfs:///output',
                                                                  '--srcPattern=.*[a-zA-Z,]+'],
                                                         'Jar': 'command-runner.jar'},
                                       'Name': 'S3DistCp step'}
