#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Create Spark cluster on EMR.

Prompt parameters:
  app               main spark script for submit spark (required)
  app-args:         arguments passed to main spark script
  aws-region:       AWS region name
  bid-price:        specify bid price for task nodes
  cluster-id:       job flow id of existing cluster to submit to
  conf-file:        specify cluster config file
  debug:            allow debugging of cluster
  dynamic-pricing:  allow sparksteps to determine best bid price for task nodes
  ec2-key:          name of the Amazon EC2 key pair
  ec2-subnet-id:    Amazon VPC subnet id
  help (-h):        argparse help
  keep-alive:       Keep EMR cluster alive when no steps
  master:           instance type of of master host (default='m4.large')
  name:             specify cluster name
  num-core:         number of core nodes
  num-task:         number of task nodes
  release-label:    EMR release label
  s3-bucket:        name of s3 bucket to upload spark file (required)
  s3-dist-cp:       s3-dist-cp step after spark job is done
  slave:            instance type of of slave hosts
  submit-args:      arguments passed to spark-submit
  tags:             EMR cluster tags of the form "key1=value1 key2=value2"
  uploads:          files to upload to /home/hadoop/ in master instance

Examples:
  sparksteps examples/episodes.py \
    --s3-bucket $AWS_S3_BUCKET \
    --aws-region us-east-1 \
    --release-label emr-4.7.0 \
    --uploads examples/lib examples/episodes.avro \
    --submit-args="--jars /home/hadoop/lib/spark-avro_2.10-2.0.2-custom.jar" \
    --app-args="--input /home/hadoop/episodes.avro" \
    --num-nodes 1 \
    --debug

"""
from __future__ import print_function

import argparse
import os
import shlex

import boto3

from sparksteps.steps import setup_steps
from sparksteps.cluster import emr_config
from sparksteps.pricing import get_bid_price


def is_valid_file(parser, arg):
    if not os.path.exists(arg):
        parser.error("The file %s does not exist!" % arg)
    return arg


def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument('app', metavar='FILE',
                        type=lambda x: is_valid_file(parser, x))
    parser.add_argument('--app-args', type=shlex.split)
    parser.add_argument('--aws-region')
    parser.add_argument('--bid-price')
    parser.add_argument('--cluster-id')
    parser.add_argument('--conf-file', metavar='FILE')
    parser.add_argument('--debug', action='store_true')
    parser.add_argument('--dynamic-pricing', action='store_true')
    parser.add_argument('--ec2-key')
    parser.add_argument('--ec2-subnet-id')
    parser.add_argument('--keep-alive', action='store_true')
    parser.add_argument('--master', default='m4.large')
    parser.add_argument('--name')
    parser.add_argument('--num-core', type=int)
    parser.add_argument('--num-task', type=int)
    parser.add_argument('--release-label')
    parser.add_argument('--s3-bucket', required=True)
    parser.add_argument('--s3-dist-cp', type=shlex.split)
    parser.add_argument('--slave')
    parser.add_argument('--submit-args', type=shlex.split)
    parser.add_argument('--tags', nargs='*')
    parser.add_argument('--uploads', nargs='*')

    args = parser.parse_args()

    client = boto3.client('emr', region_name=args.aws_region)
    s3 = boto3.resource('s3')

    cluster_id = args.cluster_id
    if cluster_id is None:  # create cluster
        print("Launching cluster...")
        args_dict = vars(args)
        if args.dynamic_pricing:
            ec2 = boto3.client('ec2', region_name=args.aws_region)
            bid_px, is_spot = get_bid_price(ec2, args.slave)
            args_dict['bid_price'] = str(bid_px)
            if is_spot:
                print("Using spot pricing with bid price ${}".format(bid_px))
            else:
                print("Spot price too high. Using on-demand ${}".format(bid_px))
        cluster_config = emr_config(**args_dict)
        response = client.run_job_flow(**cluster_config)
        cluster_id = response['JobFlowId']
        print("Cluster ID: ", cluster_id)

    emr_steps = setup_steps(s3,
                            args.s3_bucket,
                            args.app,
                            args.submit_args,
                            args.app_args,
                            args.uploads,
                            args.s3_dist_cp)

    client.add_job_flow_steps(JobFlowId=cluster_id, Steps=emr_steps)


if __name__ == '__main__':
    main()
