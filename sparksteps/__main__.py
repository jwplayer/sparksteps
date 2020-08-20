#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Create Spark cluster on EMR.

Prompt parameters:
  app                           main spark script for submit spark (required)
  app-args:                     arguments passed to main spark script
  app-list:                     Applications to be installed on the EMR cluster (Default: Hadoop Spark)
  aws-region:                   AWS region name
  bid-price:                    specify bid price for task nodes
  bootstrap-script:             include a bootstrap script (s3 path)
  cluster-id:                   job flow id of existing cluster to submit to
  debug:                        allow debugging of cluster
  defaults:                     cluster configurations of the form "<classification1> key1=val1 key2=val2 ..."
  dynamic-pricing-master:       use spot pricing for the master nodes.
  dynamic-pricing-core:         use spot pricing for the core nodes.
  dynamic-pricing-task:         use spot pricing for the task nodes.
  ebs-volume-size-core:         size of the EBS volume to attach to core nodes in GiB.
  ebs-volume-type-core:         type of the EBS volume to attach to core nodes (supported: [standard, gp2, io1]).
  ebs-volumes-per-core:         the number of EBS volumes to attach per core node.
  ebs-optimized-core:           whether to use EBS optimized volumes for core nodes.
  ebs-volume-size-task:         size of the EBS volume to attach to task nodes in GiB.
  ebs-volume-type-task:         type of the EBS volume to attach to task nodes.
  ebs-volumes-per-task:         the number of EBS volumes to attach per task node.
  ebs-optimized-task:           whether to use EBS optimized volumes for task nodes.
  ec2-key:                      name of the Amazon EC2 key pair
  ec2-subnet-id:                Amazon VPC subnet id
  help (-h):                    argparse help
  jobflow-role:                 Amazon EC2 instance profile name to use (Default: EMR_EC2_DefaultRole)
  service-role:                 AWS IAM service role to use for EMR (Default: EMR_DefaultRole)
  keep-alive:                   whether to keep the EMR cluster alive when there are no steps
  log-level (-l):               logging level (default=INFO)
  instance-type-master:         instance type of of master host (default='m4.large')
  instance-type-core:           instance type of the core nodes, must be set when num-core > 0
  instance-type-task:           instance type of the task nodes, must be set when num-task > 0
  maximize-resource-allocation: sets the maximizeResourceAllocation property for the cluster to true when supplied.
  name:                         specify cluster name
  num-core:                     number of core nodes
  num-task:                     number of task nodes
  release-label:                EMR release label
  s3-bucket:                    name of s3 bucket to upload spark file (required)
  s3-path:                      path (key prefix) within s3-bucket to use when uploading spark file
  s3-dist-cp:                   s3-dist-cp step after spark job is done
  submit-args:                  arguments passed to spark-submit
  tags:                         EMR cluster tags of the form "key1=value1 key2=value2"
  uploads:                      files to upload to /home/hadoop/ in master instance
  wait:                         poll until all steps are complete (or error)

Examples:
  sparksteps examples/episodes.py \
    --s3-bucket $AWS_S3_BUCKET \
    --aws-region us-east-1 \
    --release-label emr-4.7.0 \
    --uploads examples/lib examples/episodes.avro \
    --submit-args="--jars /home/hadoop/lib/spark-avro_2.10-2.0.2-custom.jar" \
    --app-args="--input /home/hadoop/episodes.avro" \
    --num-core 1 \
    --instance-type-core m4.large \
    --debug

"""
from __future__ import print_function

import json
import shlex
import logging
import argparse

import boto3

from sparksteps import steps
from sparksteps import cluster
from sparksteps import pricing
from sparksteps.cluster import DEFAULT_APP_LIST, DEFAULT_JOBFLOW_ROLE, DEFAULT_SERVICE_ROLE
from sparksteps.poll import wait_for_step_complete

logger = logging.getLogger(__name__)
LOGFORMAT = '%(asctime)s %(name)-12s %(levelname)-8s %(message)s'
DEFAULT_SLEEP_INTERVAL_SECONDS = 150


def create_parser():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument('app', metavar='FILE')
    parser.add_argument('--app-args', type=shlex.split)
    parser.add_argument('--app-list', nargs='*', default=DEFAULT_APP_LIST)
    parser.add_argument('--aws-region', required=True)
    parser.add_argument('--bid-price')
    parser.add_argument('--bootstrap-script')
    parser.add_argument('--cluster-id')
    parser.add_argument('--debug', action='store_true')
    parser.add_argument('--defaults', nargs='*')
    parser.add_argument('--ec2-key')
    parser.add_argument('--ec2-subnet-id')
    parser.add_argument('--jobflow-role', default=DEFAULT_JOBFLOW_ROLE)
    parser.add_argument('--service-role', default=DEFAULT_SERVICE_ROLE)
    parser.add_argument('--keep-alive', action='store_true')
    parser.add_argument('--log-level', '-l', type=str.upper, default='INFO')
    parser.add_argument('--name')
    parser.add_argument('--num-core', type=int)
    parser.add_argument('--num-task', type=int)
    parser.add_argument('--release-label', required=True)
    parser.add_argument('--s3-bucket', required=True)
    parser.add_argument('--s3-path', default='sparksteps/')
    parser.add_argument('--s3-dist-cp', type=shlex.split)
    parser.add_argument('--submit-args', type=shlex.split)
    parser.add_argument('--tags', nargs='*')
    parser.add_argument('--uploads', nargs='*')
    parser.add_argument('--maximize-resource-allocation', action='store_true')
    # TODO: wrap lines below in a for loop?
    parser.add_argument('--instance-type-master', default='m4.large')
    parser.add_argument('--instance-type-core')
    parser.add_argument('--instance-type-task')
    parser.add_argument('--dynamic-pricing-master', action='store_true')
    parser.add_argument('--dynamic-pricing-core', action='store_true')
    parser.add_argument('--dynamic-pricing-task', action='store_true')

    # EBS configuration
    parser.add_argument('--ebs-volume-size-core', type=int, default=0)
    parser.add_argument('--ebs-volume-type-core', type=str, default='standard')
    parser.add_argument('--ebs-volumes-per-core', type=int, default=1)
    parser.add_argument('--ebs-optimized-core', action='store_true')

    parser.add_argument('--ebs-volume-size-task', type=int, default=0)
    parser.add_argument('--ebs-volume-type-task', type=str, default='standard')
    parser.add_argument('--ebs-volumes-per-task', type=int, default=1)
    parser.add_argument('--ebs-optimized-task', action='store_true')

    # Wait configuration
    parser.add_argument('--wait', type=int, nargs='?', default=False)

    # Deprecated arguments
    parser.add_argument('--master')
    parser.add_argument('--slave')
    parser.add_argument('--dynamic-pricing', action='store_true')

    return parser


def parse_cli_args(parser, args=None):
    """
    Utilizes `parser` to parse command line variables and logs.
    """
    args = vars(parser.parse_args(args))

    # Perform sanitization on any arguments
    if args['s3_path'] and args['s3_path'].startswith('/'):
        raise ValueError(
            f"Provided value for s3-path \"{args['s3_path']}\" cannot have leading \"/\" character.")

    if args['wait'] is None:
        args['wait'] = DEFAULT_SLEEP_INTERVAL_SECONDS

    return args


def determine_prices(args, ec2, pricing_client):
    """
    Checks `args` in order to determine whether spot pricing should be
     used for instance groups within the EMR cluster, and if this is the
     case attempts to determine the optimal bid price.
    """
    # Check if we need to do anything
    pricing_properties = (
        'dynamic_pricing_master', 'dynamic_pricing_core', 'dynamic_pricing_task')
    if not any([x in args for x in pricing_properties]):
        return args

    # Mutate a copy of args.
    args = args.copy()

    # Determine bid prices for the instance types for which we want to
    # use bid pricing.
    for price_property in pricing_properties:
        if price_property not in args:
            continue

        if args[price_property]:
            instance_type_key = price_property.replace(
                'dynamic_pricing', 'instance_type')
            instance_type = args[instance_type_key]
            instance_group = price_property.replace('dynamic_pricing_', '')
            # TODO (rikheijdens): optimize by caching instance prices
            # between instance groups?
            bid_price, is_spot = pricing.get_bid_price(ec2, pricing_client, instance_type)
            if is_spot:
                logger.info("Using spot pricing with a bid price of $%.2f"
                            " for %s instances in the %s instance group.",
                            bid_price, instance_type,
                            instance_group)
                bid_key = price_property.replace('dynamic_pricing', 'bid_price')
                args[bid_key] = str(bid_price)
            else:
                logger.info("Spot price for %s in the %s instance group too high."
                            " Using on-demand price of $%.2f",
                            instance_type, instance_group, bid_price)
    return args


def main():
    args_dict = parse_cli_args(create_parser())
    print("Args: ", args_dict)

    numeric_level = getattr(logging, args_dict['log_level'], None)
    logging.basicConfig(format=LOGFORMAT)
    logging.getLogger('sparksteps').setLevel(numeric_level)

    client = boto3.client('emr', region_name=args_dict['aws_region'])
    s3 = boto3.resource('s3')

    cluster_id = args_dict.get('cluster_id')
    if cluster_id is None:
        logger.info("Launching cluster...")
        ec2_client = boto3.client('ec2', region_name=args_dict['aws_region'])
        pricing_client = boto3.client('pricing', region_name=args_dict['aws_region'])
        args_dict = determine_prices(args_dict, ec2_client, pricing_client)
        cluster_config = cluster.emr_config(**args_dict)
        response = client.run_job_flow(**cluster_config)
        cluster_id = response['JobFlowId']
        logger.info("Cluster ID: %s", cluster_id)

    emr_steps = steps.setup_steps(s3,
                                  args_dict['s3_bucket'],
                                  args_dict['s3_path'],
                                  args_dict['app'],
                                  args_dict['submit_args'],
                                  args_dict['app_args'],
                                  args_dict['uploads'],
                                  args_dict['s3_dist_cp'])

    response = client.add_job_flow_steps(JobFlowId=cluster_id, Steps=emr_steps)

    try:
        step_ids = json.dumps(response['StepIds'])
    except KeyError:
        step_ids = 'Invalid response'
        args_dict['wait'] = False
    logger.info("Step IDs: %s", step_ids)

    sleep_interval = args_dict.get('wait')
    if sleep_interval:
        last_step_id = response['StepIds'][-1]
        logger.info('Polling until step {last_step} is complete using a sleep interval of {interval} seconds...'
                    .format(last_step=last_step_id, interval=sleep_interval))
        wait_for_step_complete(client, cluster_id, last_step_id, sleep_interval_s=int(sleep_interval))
