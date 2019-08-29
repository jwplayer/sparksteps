# -*- coding: utf-8 -*-
"""Test Poll logic."""
import os
import pytest
import boto3

from unittest.mock import MagicMock, patch

from moto import mock_emr, mock_s3
from moto.emr.models import emr_backends

from sparksteps.cluster import emr_config
from sparksteps.poll import failure_message_from_response, is_step_complete, wait_for_step_complete


@pytest.fixture(scope='function')
def aws_credentials():
    """
    Mocked AWS Credentials for moto to prevent impact to real infrastructure
    """
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'


@pytest.fixture(scope='function')
def emr_client(aws_credentials):
    with mock_emr():
        yield boto3.client('emr', region_name='us-east-1')


@pytest.fixture(scope='function')
def s3_client(aws_credentials):
    with mock_s3():
        yield boto3.client('s3', region_name='us-east-1')


def test_failure_message_from_response():
    """
    Ensure failure_message_from_response returns expected string
    """
    mock_aws_response = {
        'Step': {
            'Status': {
                'FailureDetails': {
                    'Reason': 'error-reason',
                    'Message': 'error-message',
                    'LogFile': '/path/to/logfile'
                }
            }
        }
    }
    expected = 'for reason error-reason with message error-message and log file /path/to/logfile'
    actual = failure_message_from_response(mock_aws_response)
    assert expected == actual, 'Mismatch, Expected: {}, Actual: {}'.format(expected, actual)

    del mock_aws_response['Step']['Status']['FailureDetails']
    assert failure_message_from_response(mock_aws_response) is None, \
        'Expected None when FailureDetails key is missing from response.'


def set_step_state(step_id, cluster_id, new_state):
    """
    Helper to update the state of a step
    """
    for step in emr_backends['us-east-1'].clusters[cluster_id].steps:
        if step.id == step_id:
            step.state = new_state


def test_is_step_complete(emr_client, s3_client):
    """
    Ensure is_step_complete returns expected boolean value
    """
    cluster_config = emr_config(
        'emr-5.2.0',
        instance_type_master='m4.large',
        jobflow_role='MyCustomRole',
        keep_alive=False,
        instance_type_core='m4.2xlarge',
        instance_type_task='m4.2xlarge',
        num_core=1,
        num_task=1,
        bid_price_task='0.1',
        maximize_resource_allocation=True,
        name='Test SparkSteps',
        app_list=['hadoop', 'hive', 'spark']
    )
    response = emr_client.run_job_flow(**cluster_config)
    cluster_id = response['JobFlowId']

    test_step = {
        'Name': 'test-step',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['state-pusher-script']
        }
    }
    response = emr_client.add_job_flow_steps(JobFlowId=cluster_id, Steps=[test_step])
    last_step_id = response['StepIds'][-1]

    # while the step state is non-terminal is_step_complete should return False
    for state in ['PENDING', 'RUNNING', 'CONTINUE', 'CANCEL_PENDING']:
        set_step_state(last_step_id, cluster_id, state)
        assert not is_step_complete(emr_client, cluster_id, last_step_id), \
            'Expected last step to not be complete when step state is {}'.format(state)

    # when last step is in a terminal state (completed), is_step_complete should return True
    set_step_state(last_step_id, cluster_id, 'COMPLETED')
    assert is_step_complete(emr_client, cluster_id, last_step_id), \
        'Expected last step to be complete when last step state is {}'.format('COMPLETED')

    # when last step is in a failed state, is_step_complete should raise a helpful exception
    for state in ['CANCELLED', 'FAILED', 'INTERRUPTED']:
        set_step_state(last_step_id, cluster_id, state)
        try:
            is_step_complete(emr_client, cluster_id, last_step_id)
            assert False, \
                'Expected an exception to be raised when the last step is in {} state'.format(state)
        except Exception as e:
            assert 'EMR job failed' == str(e), 'Exception message not as expected'


def test_wait_for_step_complete():
    """
    Ensure polling.poll is called with expected arguments
    """
    with patch('sparksteps.poll.poll') as mock_poll:
        mock_emr = MagicMock()
        jobflow_id = 'fake-jobflow-id'
        step_id = 'fake-step-id'
        wait_for_step_complete(mock_emr, jobflow_id, step_id, 1)
        mock_poll.assert_called_once_with(
            is_step_complete, args=(mock_emr, jobflow_id, step_id), step=1, poll_forever=True)
