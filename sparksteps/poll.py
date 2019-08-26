# -*- coding: utf-8 -*-
"""
Utilities for polling for cluster status to determine if it's in a terminal state.
"""
import logging
from polling import poll


logger = logging.getLogger(__name__)

NON_TERMINAL_STATES = frozenset(['PENDING', 'RUNNING', 'CONTINUE', 'CANCEL_PENDING'])
FAILED_STATE = frozenset(['CANCELLED', 'FAILED', 'INTERRUPTED'])


def failure_message_from_response(response):
    """
    Given EMR response, returns a descriptive error message
    """
    fail_details = response['Step']['Status'].get('FailureDetails')
    if fail_details:
        return 'for reason {} with message {} and log file {}'\
            .format(
                fail_details.get('Reason'),
                fail_details.get('Message'),
                fail_details.get('LogFile')
            )


def is_step_complete(emr_client, jobflow_id, step_id):
    """
    Will query EMR for step status, returns True if complete, False otherwise
    """
    response = emr_client.describe_step(ClusterId=jobflow_id, StepId=step_id)

    if not response['ResponseMetadata']['HTTPStatusCode'] == 200:
        logger.info('Bad HTTP response: %s', response)
        return False

    state = response['Step']['Status']['State']
    logger.info('Job flow currently %s', state)

    if state in NON_TERMINAL_STATES:
        return False

    if state in FAILED_STATE:
        final_message = 'EMR job failed'
        failure_message = failure_message_from_response(response)
        if failure_message:
            final_message += ' ' + failure_message
        raise Exception(final_message)

    return True


def wait_for_step_complete(emr_client, jobflow_id, step_id, sleep_interval_s):
    """
    Will poll EMR until provided step has a terminal status
    """
    poll(
        is_step_complete,
        args=(emr_client, jobflow_id, step_id),
        step=sleep_interval_s,
        poll_forever=True
    )
