import os
import json
import logging
import base64

from google.cloud import pubsub_v1
from google.cloud import logging as cloud_logging

# Import shared code from your PyPI package.
from pto_common_timesheet_mfdenison_hopkinsep.models import PTO
from pto_common_timesheet_mfdenison_hopkinsep.utils.dashboard_events import build_dashboard_payload

# Initialize Google Cloud Logging
cloud_log_client = cloud_logging.Client()
cloud_log_client.setup_logging()

# Configure logger
logger = logging.getLogger("user_pto_lookup")
logger.setLevel(logging.INFO)

# Pub/Sub dashboard topic configuration
PROJECT_ID = os.environ.get("PROJECT_ID", "hopkinstimesheetproj")
DASHBOARD_TOPIC = f"projects/{PROJECT_ID}/topics/dashboard-queue"
publisher = pubsub_v1.PublisherClient()

def user_pto_lookup(event, context):
    """
    Cloud Function triggered by a Pub/Sub message to perform a PTO lookup.

    The incoming message is expected to be a JSON payload (possibly double-encoded)
    with at least the key "employee_id". The function obtains or creates a PTO record,
    builds a dashboard payload indicating the PTO balance, and publishes that payload
    to a dashboard Pub/Sub topic.

    Args:
        event (dict): The event payload. Contains the base64-encoded message data.
        context (google.cloud.functions.Context): Metadata about the event.
    """
    try:
        # Decode the base64-encoded message.
        raw_data = base64.b64decode(event["data"]).decode("utf-8")
        logger.info(f"Raw message received: {raw_data}")

        # Decode JSON. If the result is still a JSON-encoded string, parse it again.
        data = json.loads(raw_data)
        if isinstance(data, str):
            data = json.loads(data)

        employee_id = data["employee_id"]
        logger.info(f"Looking up PTO for employee_id: {employee_id}")

        # Retrieve or create the PTO record.
        # (Assumes your shared model provides a get_or_create method similar to Django.)
        pto, created = PTO.objects.get_or_create(
            employee_id=employee_id,
            defaults={"balance": 0}
        )

        if created:
            msg = f"Created new PTO record for employee_id {employee_id} with 0 balance."
            logger.info(msg)
        else:
            msg = f"PTO balance for employee_id {employee_id} is {pto.balance} hours."
            logger.info(msg)

        # Build the dashboard payload.
        payload = build_dashboard_payload(
            employee_id,
            "pto_lookup",
            msg,
            {"pto_balance": pto.balance}
        )

        # Publish the payload to the dashboard Pub/Sub topic.
        future = publisher.publish(
            DASHBOARD_TOPIC,
            json.dumps(payload).encode("utf-8")
        )
        future.result()  # Optionally, wait for the publish to complete.
        logger.info("Published PTO balance update to dashboard topic.")

    except Exception as e:
        logger.exception(f"Error processing PTO lookup message: {str(e)}")
        # Reraise the exception to signal failure (which may trigger a retry).
        raise
