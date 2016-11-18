# -*- coding: utf-8 -*-
"""Create EMR cluster."""
import collections
import datetime
import getpass
import json

from sparksteps import steps

username = getpass.getuser()

# conf if sparksteps_conf kwarg is set
# http://stackoverflow.com/a/34000524/690430
# http://stackoverflow.com/a/33118489/690430
SPARKSTEPS_CONF = [
    {
        "Classification": "spark",
        "Properties": {
            "maximizeResourceAllocation": "true"
        }
    },
]


def update_dict(d, other):
    """Recursively merge or update dict-like objects.
    >>> from pprint import pprint
    >>> pprint(update({'k1': {'k2': 2}}, {'k1': {'k2': {'k3': 3}}, 'k4': 4}))
    {'k1': {'k2': {'k3': 3}}, 'k4': 4}
    >>> pprint(update({'k1': {'k2': 2}}, {'k1': {'k3': 3}}))
    {'k1': {'k2': 2, 'k3': 3}}
    >>> pprint(update({'k1': {'k2': 2}}, dict()))
    {'k1': {'k2': 2}}

    http://stackoverflow.com/a/32357112/690430
    """

    for k, v in other.items():
        if isinstance(d, collections.Mapping):
            if isinstance(v, collections.Mapping):
                r = update_dict(d.get(k, {}), v)
                d[k] = r
            else:
                d[k] = other[k]
        else:
            d = {k: other[k]}
    return d


def parse_tags(raw_tags_list):
    """
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
        Applications=[{'Name': 'Spark'}],
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
    if kw.get('sparksteps_conf', False):
        config['Configurations'] = SPARKSTEPS_CONF
    if kw.get('ec2_key'):
        config['Instances']['Ec2KeyName'] = kw['ec2_key']
    if kw.get('ec2_subnet_id'):
        config['Instances']['Ec2SubnetId'] = kw['ec2_subnet_id']
    if kw.get('debug', False) and kw.get('s3_bucket'):
        config['LogUri'] = 's3n://%s/logs/sparksteps/' % kw['s3_bucket']
        config['Steps'] = [steps.DebugStep().step]
    if kw.get('tags'):
        config['Tags'] = parse_tags(kw['tags'])

    if kw.get('conf_file'):  # if a conf file is specified
        with open(kw.get('conf_file')) as f:
            update_dict(config, json.load(f))

    return config
