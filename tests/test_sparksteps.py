# -*- coding: utf-8 -*-
"""Test SparkSteps."""
import shlex
import os.path

import boto3
import moto

from sparksteps import __main__
from sparksteps.cluster import emr_config
from sparksteps.steps import setup_steps, S3DistCp

TEST_BUCKET = 'sparksteps-test'
AWS_REGION_NAME = 'us-east-1'

DIR_PATH = os.path.dirname(os.path.realpath(__file__))
DATA_DIR = os.path.join(DIR_PATH, 'data')
LIB_DIR = os.path.join(DATA_DIR, 'dir')
EPISODES_APP = os.path.join(DATA_DIR, 'episodes.py')
EPISODES_AVRO = os.path.join(DATA_DIR, 'episodes.avro')


@moto.mock_emr
def test_emr_cluster_config():
    config = emr_config('emr-5.2.0',
                        instance_type_master='m4.large',
                        keep_alive=False,
                        instance_type_core='m4.2xlarge',
                        instance_type_task='m4.2xlarge',
                        num_core=1,
                        num_task=1,
                        bid_price_task='0.1',
                        maximize_resource_allocation=True,
                        name="Test SparkSteps")
    assert config == {'Instances':
                          {'InstanceGroups': [{'InstanceCount': 1,  # NOQA: E127
                                               'InstanceRole': 'MASTER',
                                               'InstanceType': 'm4.large',
                                               'Market': 'ON_DEMAND',
                                               'Name': 'Master Node'},
                                              {'InstanceCount': 1,
                                               'InstanceRole': 'CORE',
                                               'InstanceType': 'm4.2xlarge',
                                               'Market': 'ON_DEMAND',
                                               'Name': 'Core Nodes'},
                                              {'BidPrice': '0.1',
                                               'InstanceCount': 1,
                                               'InstanceRole': 'TASK',
                                               'InstanceType': 'm4.2xlarge',
                                               'Market': 'SPOT',
                                               'Name': 'Task Nodes'}],
                           'KeepJobFlowAliveWhenNoSteps': False,
                           'TerminationProtected': False
                           },
                      'Applications': [{'Name': 'Hadoop'}, {'Name': 'Spark'}],
                      'Name': 'Test SparkSteps',
                      'JobFlowRole': 'EMR_EC2_DefaultRole',
                      'ReleaseLabel': 'emr-5.2.0',
                      'VisibleToAllUsers': True,
                      'ServiceRole': 'EMR_DefaultRole',
                      'Configurations': [{'Classification': 'spark',
                                          'Properties': {'maximizeResourceAllocation': 'true'}}]
                      }

    client = boto3.client('emr', region_name=AWS_REGION_NAME)
    client.run_job_flow(**config)


@moto.mock_emr
def test_emr_cluster_config_with_bootstrap():
    config = emr_config('emr-5.2.0',
                        instance_type_master='m4.large',
                        keep_alive=False,
                        instance_type_core='m4.2xlarge',
                        instance_type_task='m4.2xlarge',
                        num_core=1,
                        num_task=1,
                        bid_price_task='0.1',
                        name="Test SparkSteps",
                        bootstrap_script='s3://bucket/bootstrap-actions.sh')
    assert config == {'Instances':
                          {'InstanceGroups': [{'InstanceCount': 1,  # NOQA: E127
                                               'InstanceRole': 'MASTER',
                                               'InstanceType': 'm4.large',
                                               'Market': 'ON_DEMAND',
                                               'Name': 'Master Node'},
                                              {'InstanceCount': 1,
                                               'InstanceRole': 'CORE',
                                               'InstanceType': 'm4.2xlarge',
                                               'Market': 'ON_DEMAND',
                                               'Name': 'Core Nodes'},
                                              {'BidPrice': '0.1',
                                               'InstanceCount': 1,
                                               'InstanceRole': 'TASK',
                                               'InstanceType': 'm4.2xlarge',
                                               'Market': 'SPOT',
                                               'Name': 'Task Nodes'}],
                           'KeepJobFlowAliveWhenNoSteps': False,
                           'TerminationProtected': False
                           },
                      'Applications': [{'Name': 'Hadoop'}, {'Name': 'Spark'}],
                      'BootstrapActions': [{'Name': 'bootstrap',
                                            'ScriptBootstrapAction': {'Path': 's3://bucket/bootstrap-actions.sh'}}],
                      'Name': 'Test SparkSteps',
                      'JobFlowRole': 'EMR_EC2_DefaultRole',
                      'ReleaseLabel': 'emr-5.2.0',
                      'VisibleToAllUsers': True,
                      'ServiceRole': 'EMR_DefaultRole'}

    client = boto3.client('emr', region_name=AWS_REGION_NAME)
    client.run_job_flow(**config)


def test_emr_spot_cluster():
    config = emr_config('emr-5.2.0',
                        instance_type_master='m4.large',
                        keep_alive=False,
                        instance_type_core='c3.8xlarge',
                        instance_type_task='c3.8xlarge',
                        num_core=2,
                        num_task=4,
                        bid_price_master='0.05',
                        bid_price_core='0.25',
                        bid_price_task='0.1',
                        name="Test SparkSteps",
                        bootstrap_script='s3://bucket/bootstrap-actions.sh')
    assert config == {'Instances':
                          {'InstanceGroups': [{'InstanceCount': 1,  # NOQA: E127
                                               'InstanceRole': 'MASTER',
                                               'InstanceType': 'm4.large',
                                               'Market': 'SPOT',
                                               'BidPrice': '0.05',
                                               'Name': 'Master Node'},
                                              {'BidPrice': '0.25',
                                               'InstanceCount': 2,
                                               'InstanceRole': 'CORE',
                                               'InstanceType': 'c3.8xlarge',
                                               'Market': 'SPOT',
                                               'Name': 'Core Nodes'},
                                              {'BidPrice': '0.1',
                                               'InstanceCount': 4,
                                               'InstanceRole': 'TASK',
                                               'InstanceType': 'c3.8xlarge',
                                               'Market': 'SPOT',
                                               'Name': 'Task Nodes'}],
                           'KeepJobFlowAliveWhenNoSteps': False,
                           'TerminationProtected': False
                           },
                      'Applications': [{'Name': 'Hadoop'}, {'Name': 'Spark'}],
                      'BootstrapActions': [{'Name': 'bootstrap',
                                            'ScriptBootstrapAction': {'Path': 's3://bucket/bootstrap-actions.sh'}}],
                      'Name': 'Test SparkSteps',
                      'JobFlowRole': 'EMR_EC2_DefaultRole',
                      'ReleaseLabel': 'emr-5.2.0',
                      'VisibleToAllUsers': True,
                      'ServiceRole': 'EMR_DefaultRole'}


def test_emr_ebs_storage():
    config = emr_config('emr-5.2.0',
                        instance_type_master='m4.large',
                        keep_alive=False,
                        instance_type_core='c3.8xlarge',
                        instance_type_task='c3.8xlarge',
                        ebs_volume_size_core=100,
                        ebs_volume_type_core='gp2',
                        ebs_volumes_per_core=2,
                        ebs_volume_size_task=10,
                        ebs_volume_type_task='io1',
                        ebs_optimized_task=True,
                        num_core=2,
                        num_task=4,
                        bid_price_master='0.05',
                        bid_price_core='0.25',
                        bid_price_task='0.1',
                        name="Test SparkSteps",
                        bootstrap_script='s3://bucket/bootstrap-actions.sh')
    assert config == {'Instances':
                          {'InstanceGroups': [{'InstanceCount': 1,  # NOQA: E127
                                               'InstanceRole': 'MASTER',
                                               'InstanceType': 'm4.large',
                                               'Market': 'SPOT',
                                               'BidPrice': '0.05',
                                               'Name': 'Master Node'},
                                              {'BidPrice': '0.25',
                                               'InstanceCount': 2,
                                               'InstanceRole': 'CORE',
                                               'InstanceType': 'c3.8xlarge',
                                               'Market': 'SPOT',
                                               'Name': 'Core Nodes',
                                               'EbsConfiguration': {
                                               'EbsBlockDeviceConfigs': [{
                                                    'VolumeSpecification': {
                                                        'VolumeType': 'gp2',
                                                        'SizeInGB': 100
                                                    },
                                                    'VolumesPerInstance': 2
                                                }],
                                                'EbsOptimized': False
                                               }},
                                              {'BidPrice': '0.1',
                                               'InstanceCount': 4,
                                               'InstanceRole': 'TASK',
                                               'InstanceType': 'c3.8xlarge',
                                               'Market': 'SPOT',
                                               'Name': 'Task Nodes',
                                               'EbsConfiguration': {
                                               'EbsBlockDeviceConfigs': [{
                                                    'VolumeSpecification': {
                                                        'VolumeType': 'io1',
                                                        'SizeInGB': 10
                                                    },
                                                    'VolumesPerInstance': 1
                                                }],
                                                'EbsOptimized': True
                                               }}],
                           'KeepJobFlowAliveWhenNoSteps': False,
                           'TerminationProtected': False
                           },
                      'Applications': [{'Name': 'Hadoop'}, {'Name': 'Spark'}],
                      'BootstrapActions': [{'Name': 'bootstrap',
                                            'ScriptBootstrapAction': {'Path': 's3://bucket/bootstrap-actions.sh'}}],
                      'Name': 'Test SparkSteps',
                      'JobFlowRole': 'EMR_EC2_DefaultRole',
                      'ReleaseLabel': 'emr-5.2.0',
                      'VisibleToAllUsers': True,
                      'ServiceRole': 'EMR_DefaultRole'}


@moto.mock_s3
def test_setup_steps():
    s3 = boto3.resource('s3', region_name=AWS_REGION_NAME)
    s3.create_bucket(Bucket=TEST_BUCKET)
    steps = (setup_steps(s3,
                         TEST_BUCKET,
                         EPISODES_APP,
                         submit_args="--jars /home/hadoop/dir/test.jar".split(),
                         app_args="--input /home/hadoop/episodes.avro".split(),
                         uploads=[LIB_DIR, EPISODES_AVRO])
             )
    assert steps == [
        {'HadoopJarStep': {'Jar': 'command-runner.jar',
                           'Args': ['aws', 's3', 'cp',
                                    's3://sparksteps-test/sparksteps/sources/dir.zip',
                                    '/home/hadoop/']},
         'ActionOnFailure': 'CANCEL_AND_WAIT',
         'Name': 'Copy dir.zip'},
        {'HadoopJarStep': {'Jar': 'command-runner.jar',
                           'Args': ['unzip', '-o', '/home/hadoop/dir.zip',
                                    '-d', '/home/hadoop/dir']},
         'ActionOnFailure': 'CANCEL_AND_WAIT',
         'Name': 'Unzip dir.zip'},
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
                                    '/home/hadoop/dir/test.jar',
                                    '/home/hadoop/episodes.py', '--input',
                                    '/home/hadoop/episodes.avro']},
         'ActionOnFailure': 'CANCEL_AND_WAIT',
         'Name': 'Run episodes.py'}]


def test_s3_dist_cp_step():
    splitted = shlex.split(
        "--s3Endpoint=s3.amazonaws.com --src=s3://mybucket/logs/j-3GYXXXXXX9IOJ/node/ --dest=hdfs:///output --srcPattern=.*[a-zA-Z,]+")  # NOQA: E501
    assert S3DistCp(splitted).step == {
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Args': ['s3-dist-cp',
                     '--s3Endpoint=s3.amazonaws.com',
                     '--src=s3://mybucket/logs/j-3GYXXXXXX9IOJ/node/',
                     '--dest=hdfs:///output',
                     '--srcPattern=.*[a-zA-Z,]+'],
            'Jar': 'command-runner.jar'},
        'Name': 'S3DistCp step'
    }


def test_parser():
    parser = __main__.create_parser()
    cmd_args_str = """episodes.py \
      --s3-bucket my-bucket \
      --aws-region us-east-1 \
      --release-label emr-4.7.0 \
      --uploads examples/dir examples/episodes.avro \
      --submit-args="--jars /home/hadoop/lib/spark-avro_2.10-2.0.2.jar" \
      --app-args="--input /home/hadoop/episodes.avro" \
      --num-core 1 \
      --tags Name=MyName CostCenter=MyCostCenter \
      --defaults key=value another_key=another_value \
      --maximize-resource-allocation \
      --debug
    """
    args = __main__.parse_cli_args(parser, args=shlex.split(cmd_args_str))
    assert args['app'] == 'episodes.py'
    assert args['s3_bucket'] == 'my-bucket'
    assert args['app_args'] == ['--input', '/home/hadoop/episodes.avro']
    assert args['debug'] is True
    assert args['defaults'] == ['key=value', 'another_key=another_value']
    assert args['instance_type_master'] == 'm4.large'
    assert args['release_label'] == 'emr-4.7.0'
    assert args['submit_args'] == ['--jars',
                                   '/home/hadoop/lib/spark-avro_2.10-2.0.2.jar']
    assert args['uploads'] == ['examples/dir', 'examples/episodes.avro']
    assert args['tags'] == ['Name=MyName', 'CostCenter=MyCostCenter']
    assert args['maximize_resource_allocation'] is True
    assert args['num_core'] == 1


def test_parser_deprecated_args():
    parser = __main__.create_parser()
    cmd_args_str = """episodes.py \
      --s3-bucket my-bucket \
      --aws-region us-east-1 \
      --release-label emr-4.7.0 \
      --uploads examples/dir examples/episodes.avro \
      --submit-args="--jars /home/hadoop/lib/spark-avro_2.10-2.0.2.jar" \
      --app-args="--input /home/hadoop/episodes.avro" \
      --master m4.4xlarge \
      --slave c3.8xlarge \
      --num-core 1 \
      --dynamic-pricing \
      --tags Name=MyName CostCenter=MyCostCenter \
      --defaults key=value another_key=another_value \
      --maximize-resource-allocation \
      --debug
    """
    args = __main__.parse_cli_args(parser, args=shlex.split(cmd_args_str))
    assert args['app'] == 'episodes.py'
    assert args['s3_bucket'] == 'my-bucket'
    assert args['app_args'] == ['--input', '/home/hadoop/episodes.avro']
    assert args['debug'] is True
    assert args['defaults'] == ['key=value', 'another_key=another_value']
    assert args['instance_type_master'] == 'm4.4xlarge'
    assert args['instance_type_core'] == 'c3.8xlarge'
    assert args['dynamic_pricing_task'] is True
    assert args['release_label'] == 'emr-4.7.0'
    assert args['submit_args'] == ['--jars',
                                   '/home/hadoop/lib/spark-avro_2.10-2.0.2.jar']
    assert args['uploads'] == ['examples/dir', 'examples/episodes.avro']
    assert args['tags'] == ['Name=MyName', 'CostCenter=MyCostCenter']
    assert args['maximize_resource_allocation'] is True


def test_parser_with_bootstrap():
    parser = __main__.create_parser()
    cmd_args_str = """episodes.py \
      --s3-bucket my-bucket \
      --aws-region us-east-1 \
      --release-label emr-4.7.0 \
      --uploads examples/dir examples/episodes.avro \
      --submit-args="--jars /home/hadoop/lib/spark-avro_2.10-2.0.2.jar" \
      --app-args="--input /home/hadoop/episodes.avro" \
      --num-core 1 \
      --tags Name=MyName CostCenter=MyCostCenter \
      --defaults key=value another_key=another_value \
      --bootstrap-script s3://bucket/bootstrap-actions.sh \
      --debug
    """
    args = __main__.parse_cli_args(parser, args=shlex.split(cmd_args_str))
    assert args['app'] == 'episodes.py'
    assert args['s3_bucket'] == 'my-bucket'
    assert args['app_args'] == ['--input', '/home/hadoop/episodes.avro']
    assert args['debug'] is True
    assert args['defaults'] == ['key=value', 'another_key=another_value']
    assert args['instance_type_master'] == 'm4.large'
    assert args['release_label'] == 'emr-4.7.0'
    assert args['submit_args'] == ['--jars',
                                   '/home/hadoop/lib/spark-avro_2.10-2.0.2.jar']
    assert args['uploads'] == ['examples/dir', 'examples/episodes.avro']
    assert args['tags'] == ['Name=MyName', 'CostCenter=MyCostCenter']
    assert args['bootstrap_script'] == 's3://bucket/bootstrap-actions.sh'
