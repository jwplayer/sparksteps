# -*- coding: utf-8 -*-
"""Create EMR cluster."""
import collections
import datetime
import getpass
import json

from sparksteps import steps

username = getpass.getuser()

# conf if args.sparksteps_conf is set
SPARKSTEPS_CONF = [
    {
        "Classification": "capacity-scheduler",
        "Properties": {
            "yarn.scheduler.capacity.resource-calculator": "org.apache.hadoop.yarn.util.resource.DominantResourceCalculator"
        }
    },
    {
        'Classification': 'spark-defaults',
        'Properties': {
            'spark.dynamicAllocation.enabled': 'true',
            'spark.executor.instances': '0',
            'spark.shuffle.memoryFraction': '0.5',
            'spark.yarn.executor.memoryOverhead': '1024',
        },
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


def emr_config(release_label, master, slave, num_nodes, keep_alive=False, **kw):
    timestamp = datetime.datetime.now().replace(microsecond=0)
    # http://stackoverflow.com/a/34000524/690430
    # http://stackoverflow.com/a/33118489/690430
    config = dict(
        Name="{} SparkStep Task [{}]".format(username, timestamp),
        ReleaseLabel=release_label,
        Instances={
            'MasterInstanceType': master,
            'SlaveInstanceType': slave,
            'InstanceCount': num_nodes,
            'KeepJobFlowAliveWhenNoSteps': keep_alive,
            'TerminationProtected': False,
        },
        Applications=[
            {
                'Name': 'Spark'
            }
        ],
        VisibleToAllUsers=True,
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='EMR_DefaultRole',
    )
    if kw.get('sparksteps_conf', False):
        config['Configurations'] = SPARKSTEPS_CONF
    if kw.get('ec2_key'):
        config['Instances']['Ec2KeyName'] = kw.get('ec2_key')
    if kw.get('ec2_subnet_id'):
        config['Instances']['Ec2SubnetId'] = kw.get('ec2_subnet_id')
    if kw.get('debug', False) and kw.get('s3_bucket'):
        config['LogUri'] = 's3n://%s/logs/sparksteps/' % kw.get('s3_bucket')
        config['Steps'] = [steps.DebugStep().step]
    if kw.get('tags'):
        config['Tags'] = parse_tags(kw.get('tags'))

    additional_config = dict()
    if kw.get('conf_file'):  # if a conf file is specified
        with open(kw.get('conf_file')) as f:
            additional_config = json.load(f)

    update_dict(config, additional_config)

    return config
