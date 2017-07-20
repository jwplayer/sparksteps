# -*- coding: utf-8 -*-
"""Create EMR cluster."""
import getpass
import logging
import datetime

from sparksteps import steps

logger = logging.getLogger(__name__)

username = getpass.getuser()


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
    if kw.get('bootstrap_script'):
        config['BootstrapActions'] = [{'Name': 'bootstrap',
                                       'ScriptBootstrapAction': {'Path': kw['bootstrap_script']}}]

    return config
