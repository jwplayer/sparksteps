# -*- coding: utf-8 -*-
"""Test Parser."""
import shlex
from sparksteps import __main__


def test_parser():
    parser = __main__.create_parser()
    cmd_args_str = """episodes.py \
      --jobflow-role MyCustomRole \
      --s3-bucket my-bucket \
      --aws-region us-east-1 \
      --release-label emr-4.7.0 \
      --uploads examples/dir examples/episodes.avro \
      --submit-args="--jars /home/hadoop/lib/spark-avro_2.10-2.0.2.jar" \
      --app-args="--input /home/hadoop/episodes.avro" \
      --app-list Hadoop Hive Spark \
      --num-core 1 \
      --tags Name=MyName CostCenter=MyCostCenter \
      --defaults spark-defaults key=value another_key=another_value \
      --maximize-resource-allocation \
      --debug \
      --wait
    """
    args = __main__.parse_cli_args(parser, args=shlex.split(cmd_args_str))
    assert args['app'] == 'episodes.py'
    assert args['jobflow_role'] == 'MyCustomRole'
    assert args['s3_bucket'] == 'my-bucket'
    assert args['app_args'] == ['--input', '/home/hadoop/episodes.avro']
    assert args['app_list'] == ['Hadoop', 'Hive', 'Spark']
    assert args['debug'] is True
    assert args['defaults'] == ['spark-defaults', 'key=value', 'another_key=another_value']
    assert args['instance_type_master'] == 'm4.large'
    assert args['release_label'] == 'emr-4.7.0'
    assert args['submit_args'] == ['--jars',
                                   '/home/hadoop/lib/spark-avro_2.10-2.0.2.jar']
    assert args['uploads'] == ['examples/dir', 'examples/episodes.avro']
    assert args['tags'] == ['Name=MyName', 'CostCenter=MyCostCenter']
    assert args['maximize_resource_allocation'] is True
    assert args['num_core'] == 1
    assert args['wait'] == 150


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
      --defaults spark-defaults key=value another_key=another_value \
      --bootstrap-script s3://bucket/bootstrap-actions.sh \
      --debug
    """
    args = __main__.parse_cli_args(parser, args=shlex.split(cmd_args_str))
    assert args['app'] == 'episodes.py'
    assert args['s3_bucket'] == 'my-bucket'
    assert args['app_args'] == ['--input', '/home/hadoop/episodes.avro']
    assert args['debug'] is True
    assert args['defaults'] == ['spark-defaults', 'key=value', 'another_key=another_value']
    assert args['instance_type_master'] == 'm4.large'
    assert args['release_label'] == 'emr-4.7.0'
    assert args['submit_args'] == ['--jars',
                                   '/home/hadoop/lib/spark-avro_2.10-2.0.2.jar']
    assert args['uploads'] == ['examples/dir', 'examples/episodes.avro']
    assert args['tags'] == ['Name=MyName', 'CostCenter=MyCostCenter']
    assert args['bootstrap_script'] == 's3://bucket/bootstrap-actions.sh'
