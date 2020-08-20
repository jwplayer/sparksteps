# -*- coding: utf-8 -*-
"""Get optimal pricing for EC2 instances."""
import json
import datetime
import itertools
import logging
import collections

logger = logging.getLogger(__name__)

SPOT_DEMAND_THRESHOLD_FACTOR = 0.8
SPOT_PRICE_LOOKBACK = 12  # hours

Zone = collections.namedtuple('Zone', 'name max min mean current')
Spot = collections.namedtuple('Spot', 'availability_zone timestamp price')

EC2_PRICE_FILTER_TEMPLATE = '''
[
    {{"Field": "tenancy", "Value": "shared", "Type": "TERM_MATCH"}},
    {{"Field": "operatingSystem", "Value": "{operating_sytem}", "Type": "TERM_MATCH"}},
    {{"Field": "preInstalledSw", "Value": "NA", "Type": "TERM_MATCH"}},
    {{"Field": "instanceType", "Value": "{instance_type}", "Type": "TERM_MATCH"}},
    {{"Field": "location", "Value": "{region}", "Type": "TERM_MATCH"}},
    {{"Field": "licenseModel", "Value": "No License required", "Type": "TERM_MATCH"}},
    {{"Field": "usagetype", "Value": "BoxUsage:{instance_type}", "Type": "TERM_MATCH"}}
]
'''


def get_demand_price(pricing_client, instance_type, region='US East (N. Virginia)', operating_system='Linux'):
    """
    Retrieves the on-demand price for a particular EC2 instance type in the specified region.
    This function does not take reserved instance pricing into account.

    Args:
        pricing_client: Boto3 Pricing client.
        instance_type (str): The type of the instance.
        region: The region to get the price for, this must be the human-readable name of the region!
        operating_system: The operating system of the instance, this must be the human-readable name!
    """
    if '-' in region:
        # TODO (rikheijdens): Perhaps we could map these using information from botocore/data/endpoints.json.
        raise ValueError('get_demand_price() requires the human-readable name of the region to be supplied.')

    filter_template = EC2_PRICE_FILTER_TEMPLATE.format(
        operating_sytem=operating_system, instance_type=instance_type, region=region)
    data = pricing_client.get_products(ServiceCode='AmazonEC2', Filters=json.loads(filter_template))
    on_demand = json.loads(data['PriceList'][0])['terms']['OnDemand']
    index_1 = list(on_demand)[0]
    index_2 = list(on_demand[index_1]['priceDimensions'])[0]
    return float(on_demand[index_1]['priceDimensions'][index_2]['pricePerUnit']['USD'])


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
    # We always bid higher than the maximum current spot price for a particular instance type
    # in order to make it less likely that our clusters will be shutdown.
    return min(1.2 * aws_zone.max, demand_price * SPOT_DEMAND_THRESHOLD_FACTOR), True


def get_bid_price(ec2_client, pricing_client, instance_type):
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
    history = get_spot_price_history(ec2_client, instance_type, SPOT_PRICE_LOOKBACK)
    by_zone = price_by_zone(history)
    zone_profile = get_zone_profile(by_zone)
    best_zone = min(zone_profile, key=lambda x: x.max)
    demand_price = get_demand_price(pricing_client, instance_type)
    bid_price, is_spot = determine_best_price(demand_price, best_zone)
    bid_price_rounded = round(bid_price, 2)  # AWS requires max 3 decimal places
    return bid_price_rounded, is_spot
