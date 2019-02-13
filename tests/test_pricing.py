"""
Unit/Integration Tests for the pricing module.

Integration Tests (i.e tests that perform actual queries / make HTTP requests)
 are marked appropriately using PyTest markers.
"""
import pytest

import boto3

from sparksteps.pricing import get_bid_price, get_demand_price

# The price for an m4.large on-demand Linux instance in us-east-1.
M4_LARGE_OD_PRICE = 0.100000


@pytest.fixture
def ec2():
    """
    In order to test pricing mechanics, we need to be able to make actual requests AWS.
    Since we're actually communicating with AWS here this makes this tests using this fixture
     more of an integration test than a unit test.
    """
    client = boto3.client('ec2')
    return client


@pytest.fixture
def pricing_client():
    """
    Boto3 Pricing Client.
    """
    return boto3.client('pricing')


@pytest.mark.integration
class TestPricing:
    def test_get_demand_price(self, pricing_client):
        price = get_demand_price(pricing_client, 'm4.large')
        # Note: this test assumes that AWS doesn't
        # change their on-demand price.
        assert price == M4_LARGE_OD_PRICE

    def test_get_bid_price(self, ec2, pricing_client):
        bid_price, is_spot = get_bid_price(ec2, pricing_client, 'm4.large')
        if is_spot:
            assert bid_price > 0.
        else:
            assert bid_price == get_demand_price('us-east-1', 'm4.large')
