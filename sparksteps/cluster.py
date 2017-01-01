# -*- coding: utf-8 -*-
"""Create EMR cluster."""

import json
import getpass
import logging
import datetime
import collections

from sparksteps import steps

logger = logging.getLogger(__name__)

username = getpass.getuser()

INVALID_KEYS = frozenset(
    ['MasterInstanceType', 'SlaveInstanceType', 'InstanceCount']
)


def update_dict(d, other, override=False):
    """Recursively merge or update dict-like objects.
    http://stackoverflow.com/a/32357112/690430

    Examples:
        >>> from pprint import pprint
        >>> pprint(update_dict({'k1': {'k2': 2}}, {'k1': {'k2': {'k3': 3}}, 'k4': 4}, override=False))
        {'k1': {'k2': 2}, 'k4': 4}
        >>> pprint(update_dict({'k1': {'k2': 2}}, {'k1': {'k3': 3}}, override=False))
        {'k1': {'k2': 2}}
        >>> pprint(update_dict({'k1': {'k2': 2}}, dict(), override=False))
        {'k1': {'k2': 2}}
        >>> pprint(update_dict({'k1': {'k2': 2}}, {'k1': {'k2': {'k3': 3}}, 'k4': 4}, override=True))
        {'k1': {'k2': {'k3': 3}}, 'k4': 4}
        >>> pprint(update_dict({'k1': {'k2': 2}}, {'k1': {'k3': 3}}, override=True))
        {'k1': {'k2': 2, 'k3': 3}}
        >>> pprint(update_dict({'k1': {'k2': 2}}, dict(), override=True))
        {'k1': {'k2': 2}}
    """  # NOQA: E501

    for k, v in other.items():
        if isinstance(d, collections.Mapping):
            if k not in d or override:
                if isinstance(v, collections.Mapping):
                    r = update_dict(d.get(k, {}), v, override)
                    d[k] = r
                else:
                    d[k] = other[k]
        else:
            d = {k: other[k]}
    return d


def parse_tags(raw_tags_list):
    """Parse AWS tags.

    Examples:
        >>> from pprint import pprint
        >>> pprint(parse_tags(['name="Peanut Pug"', 'age=5']))
        [{'Key': 'name', 'Value': '"Peanut Pug"'}, {'Key': 'age', 'Value': '5'}]
    """
    tags_dict_list = []
    for raw_tag in raw_tags_list:
        if raw_tag.find('=') == -1:
            key, value = raw_tag, ''
        else:
            key, value = raw_tag.split('=', 1)
        tags_dict_list.append({'Key': key, 'Value': value})

    return tags_dict_list


def emr_config(release_label, master, keep_alive=False, **kw):
    timestamp = datetime.datetime.now().replace(microsecond=0)
    config = dict(
        Name="{} SparkStep Task [{}]".format(username, timestamp),
        ReleaseLabel=release_label,
        Instances={
            'InstanceGroups': [
                {
                    'Name': 'Master Node',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': master,
                    'InstanceCount': 1,
                },
            ],
            'KeepJobFlowAliveWhenNoSteps': keep_alive,
            'TerminationProtected': False,
        },
        Applications=[{'Name': 'Hadoop'}, {'Name': 'Spark'}],
        VisibleToAllUsers=True,
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='EMR_DefaultRole',
    )
    if kw.get('slave'):
        if kw.get('num_core'):
            config['Instances']['InstanceGroups'].append({
                'Name': 'Core Nodes',
                'Market': 'ON_DEMAND',
                'InstanceRole': 'CORE',
                'InstanceType': kw['slave'],
                'InstanceCount': kw['num_core'],
            })
        if kw.get('num_task'):
            task_group = {
                'Name': 'Task Nodes',
                'Market': 'ON_DEMAND',
                'InstanceRole': 'TASK',
                'InstanceType': kw['slave'],
                'InstanceCount': kw['num_task'],
            }
            if kw.get('bid_price'):
                task_group['Market'] = 'SPOT'
                task_group['BidPrice'] = kw['bid_price']
            config['Instances']['InstanceGroups'].append(task_group)
    if kw.get('name'):
        config['Name'] = kw['name']
    if kw.get('ec2_key'):
        config['Instances']['Ec2KeyName'] = kw['ec2_key']
    if kw.get('ec2_subnet_id'):
        config['Instances']['Ec2SubnetId'] = kw['ec2_subnet_id']
    if kw.get('debug', False) and kw.get('s3_bucket'):
        config['LogUri'] = 's3://%s/logs/sparksteps/' % kw['s3_bucket']
        config['Steps'] = [steps.DebugStep().step]
    if kw.get('tags'):
        config['Tags'] = parse_tags(kw['tags'])

    if kw.get('conf_file'):  # if a conf file is specified
        with open(kw.get('conf_file')) as f:
            update_dict(config, json.load(f), override=False)
        if any([k in config for k in INVALID_KEYS]):
            raise Exception("Detected key from %s. "
                            "You must use InstanceGroups", INVALID_KEYS)

    return config
