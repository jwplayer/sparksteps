# -*- coding: utf-8 -*-
"""Create EMR cluster."""
import os
import getpass
import logging
import datetime

from sparksteps import steps

DEFAULT_JOBFLOW_ROLE = 'EMR_EC2_DefaultRole'
DEFAULT_SERVICE_ROLE = 'EMR_DefaultRole'
DEFAULT_APP_LIST = ['Hadoop', 'Spark']

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


def parse_conf(raw_conf_list):
    """Parse configuration items."""

    defaults = []
    classification = None

    for token in raw_conf_list:
        if '=' in token:
            key, value = token.split('=', 1)
            classification['Properties'][key] = value
        else:
            if classification:
                defaults.append(classification)
            classification = {
                'Classification': token,
                'Properties': {}
            }

    if classification:
        defaults.append(classification)

    return defaults


def parse_apps(raw_app_list):
    """
    Given a list of app name strings,
    returns formatted application configuration value.

    Examples:
        >>> from pprint import pprint
        >>> pprint(parse_apps(['hadoop', 'spark']))
        [{'Name': 'Hadoop', 'Name': 'Spark'}]
    """
    return sorted(
        [{'Name': app_name.capitalize()} for app_name in set(raw_app_list)],
        key=lambda x: x['Name'])


def emr_config(release_label, keep_alive=False, **kw):
    timestamp = datetime.datetime.now().replace(microsecond=0)
    config = dict(
        Name="{} SparkStep Task [{}]".format(username, timestamp),
        ReleaseLabel=release_label,
        Instances={
            'InstanceGroups': [],
            'KeepJobFlowAliveWhenNoSteps': keep_alive,
            'TerminationProtected': False,
        },
        Applications=parse_apps(kw.get('app_list', DEFAULT_APP_LIST)),
        VisibleToAllUsers=True,
        JobFlowRole=kw.get('jobflow_role', DEFAULT_JOBFLOW_ROLE),
        ServiceRole=kw.get('service_role', DEFAULT_SERVICE_ROLE)
    )

    for instance_group in ('master', 'core', 'task'):
        num_instances = kw.get('num_{}'.format(instance_group), 0)
        if instance_group != 'master' and not num_instances:
            # We don't need this instance group.
            continue

        instance_type = kw.get('instance_type_{}'.format(instance_group))
        if not instance_type:
            raise ValueError('{} nodes specified without instance type.'.format(
                instance_group.capitalize()))

        instance_group_config = {
            'Name': '{} Node{}'.format(instance_group.capitalize(),
                                       's' if instance_group != 'master' else ''),
            'Market': 'ON_DEMAND',
            'InstanceRole': instance_group.upper(),
            'InstanceType': instance_type,
            'InstanceCount': 1 if instance_group == 'master' else num_instances
        }

        bid_price = kw.get('bid_price_{}'.format(instance_group))
        if bid_price:
            instance_group_config['Market'] = 'SPOT'
            instance_group_config['BidPrice'] = bid_price

        ebs_volume_size = kw.get('ebs_volume_size_{}'.format(instance_group), 0)
        if ebs_volume_size:
            ebs_configuration = {
                'EbsBlockDeviceConfigs': [{
                    'VolumeSpecification': {
                        'VolumeType': kw.get('ebs_volume_type_{}'.format(instance_group)),
                        'SizeInGB': ebs_volume_size
                    },
                    'VolumesPerInstance': kw.get('ebs_volumes_per_{}'.format(instance_group), 1)
                }],
                'EbsOptimized': kw.get('ebs_optimized_{}'.format(instance_group), False)
            }
            instance_group_config['EbsConfiguration'] = ebs_configuration
        config['Instances']['InstanceGroups'].append(instance_group_config)

    if kw.get('name'):
        config['Name'] = kw['name']
    if kw.get('ec2_key'):
        config['Instances']['Ec2KeyName'] = kw['ec2_key']
    if kw.get('ec2_subnet_id'):
        config['Instances']['Ec2SubnetId'] = kw['ec2_subnet_id']
    if kw.get('debug', False) and kw.get('s3_bucket'):
        config['LogUri'] = os.path.join('s3://', kw['s3_bucket'], kw['s3_path'], 'logs/')
        config['Steps'] = [steps.DebugStep().step]
    if kw.get('tags'):
        config['Tags'] = parse_tags(kw['tags'])
    if kw.get('defaults'):
        config['Configurations'] = parse_conf(kw['defaults'])
    if kw.get('maximize_resource_allocation'):
        configurations = config.get('Configurations', [])
        configurations.append({
            'Classification': 'spark',
            'Properties': {'maximizeResourceAllocation': 'true'}
        })
        config['Configurations'] = configurations
    if kw.get('bootstrap_script'):
        config['BootstrapActions'] = [{'Name': 'bootstrap',
                                       'ScriptBootstrapAction': {'Path': kw['bootstrap_script']}}]

    return config
