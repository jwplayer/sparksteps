# -*- coding: utf-8 -*-
"""Get optimal pricing for EC2 instances."""
import json
import datetime
import itertools
import logging
import collections

from bs4 import BeautifulSoup
from six.moves.urllib.request import urlopen

logger = logging.getLogger(__name__)

EC2_INSTANCES_INFO_URL = "http://www.ec2instances.info/"
SPOT_DEMAND_THRESHOLD_FACTOR = 0.8
SPOT_PRICE_LOOKBACK = 12  # hours

Zone = collections.namedtuple('Zone', 'name max min mean current')
Spot = collections.namedtuple('Spot', 'availability_zone timestamp price')


def get_demand_price(aws_region, instance_type):
    """Get AWS instance demand price.

    >>> print(get_demand_price('us-east-1', 'm4.2xlarge'))
    """
    soup = BeautifulSoup(urlopen(EC2_INSTANCES_INFO_URL), 'html.parser')
    table = soup.find('table', {'id': 'data'})
    row = table.find(id=instance_type)
    td = row.find('td', {'class': 'cost-ondemand-linux'})
    region_prices = json.loads(td['data-pricing'])
    return float(region_prices[aws_region])


def get_spot_price_history(ec2_client, instance_type, lookback=1):
    """Return dictionary of price history by availability zone.

    Args:
        ec2_client: EC2 client
        instance_type (str): get results by the specified instance type
        lookback (int): number of hours to look back for spot history

    Returns:
        float: bid price for the instance type.
    """
    end = datetime.datetime.utcnow()
    start = end - datetime.timedelta(hours=lookback)

    response = ec2_client.describe_spot_price_history(
        StartTime=start,
        EndTime=end,
        InstanceTypes=[
            instance_type,
        ],
        ProductDescriptions=[
            'Linux/UNIX (Amazon VPC)',
            'Linux/UNIX',
        ],
    )
    return response['SpotPriceHistory']


def price_by_zone(price_history):
    prices = [Spot(d['AvailabilityZone'], d['Timestamp'], float(d['SpotPrice']))
              for d in price_history]
    g = itertools.groupby(sorted(prices), key=lambda x: x.availability_zone)
    result = {key: list(grp) for key, grp in g}
    return result


def get_zone_profile(zone_history):
    zone_prices = {k: [x.price for x in v] for k, v in zone_history.items()}
    return [Zone(k, max(v), min(v), sum(v) / len(v), v[-1])
            for k, v in zone_prices.items()]


def determine_best_price(demand_price, aws_zone):
    """Calculate optimal bid price.

    Args:
        demand_price (float): on-demand cost of AWS instance
        aws_zone (Zone): AWS zone namedtuple ('name max min mean current')

    Returns:
        float: bid price
        bool: boolean to use spot pricing
    """
    if aws_zone.current >= demand_price * SPOT_DEMAND_THRESHOLD_FACTOR:
        return demand_price, False
    return min(2 * aws_zone.max, demand_price * 0.5), True


def get_bid_price(client, instance_type):
    """Determine AWS bid price.

    Args:
        client: boto3 client
        instance_type: EC2 instance type

    Returns:
        float: bid price, bool: is stop

    Examples:
        >>> import boto3
        >>> client = boto3.client('ec2', region_name='us-east-1')
        >>> print(get_bid_price(client, 'm3.2xlarge'))
    """
    aws_region = client._client_config.region_name
    history = get_spot_price_history(client, instance_type, SPOT_PRICE_LOOKBACK)
    by_zone = price_by_zone(history)
    zone_profile = get_zone_profile(by_zone)
    best_zone = min(zone_profile, key=lambda x: x.max)
    demand_price = get_demand_price(aws_region, instance_type)
    bid_price, is_spot = determine_best_price(demand_price, best_zone)
    bid_price_rounded = round(bid_price, 2)  # AWS requires max 3 decimal places
    return bid_price_rounded, is_spot
