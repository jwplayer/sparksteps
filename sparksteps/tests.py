# -*- coding: utf-8 -*-
"""Test SparkSteps."""

import boto3
import moto

from sparksteps.steps import setup_steps
from sparksteps.cluster import emr_config

TEST_BUCKET = 'sparksteps-test'


@moto.mock_emr
def test_emr_config():
    config = emr_config('emr-4.6.0', master='m4.large', slave='m4.2xlarge',
                        num_nodes=1, keep_alive=False,
                        conf_file='examples/cluster.json')

    assert config == {'ReleaseLabel': 'emr-4.7.0',
                      'Name': 'Test SparkSteps',
                      'VisibleToAllUsers': True,
                      'Configurations': [{
                          'Properties': {
                              'spark.executor.instances': '1',
                              'spark.dynamicAllocation.enabled': 'false'},
                          'Classification': 'spark-defaults'}],
                      'JobFlowRole': 'EMR_EC2_DefaultRole',
                      'Applications': [{'Name': 'Spark'}],
                      'Instances': {'InstanceCount': 1,
                                    'TerminationProtected': True,
                                    'MasterInstanceType': 'm4.large',
                                    'KeepJobFlowAliveWhenNoSteps': False,
                                    'SlaveInstanceType': 'm4.2xlarge'},
                      'ServiceRole': 'EMR_DefaultRole'}

    client = boto3.client('emr', region_name='us-east-1')
    client.run_job_flow(**config)


@moto.mock_s3
def test_setup_steps():
    s3 = boto3.resource('s3', region_name='us-east-1')
    s3.create_bucket(Bucket=TEST_BUCKET)
    steps = (setup_steps(s3,
                         TEST_BUCKET,
                         'examples/episodes.py',
                         submit_args="--jars /home/hadoop/lib/spark-avro_2.10-2.0.2-custom.jar".split(),
                         app_args="--input /home/hadoop/episodes.avro".split(),
                         uploads=['examples/lib', 'examples/episodes.avro'])
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
