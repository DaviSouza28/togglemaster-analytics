import importlib
import sys
import threading
from unittest.mock import MagicMock

import boto3
import pytest


@pytest.fixture
def app_module(monkeypatch):
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv(
        "AWS_SQS_URL",
        "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
    )
    monkeypatch.setenv(
        "AWS_DYNAMODB_TABLE",
        "ToggleMasterAnalytics",
    )

    fake_sqs = MagicMock()
    fake_dynamodb = MagicMock()

    fake_session = MagicMock()

    def fake_client(service_name):
        if service_name == "sqs":
            return fake_sqs

        if service_name == "dynamodb":
            return fake_dynamodb

        raise ValueError(f"Unexpected AWS service: {service_name}")

    fake_session.client.side_effect = fake_client

    monkeypatch.setattr(
        boto3,
        "Session",
        MagicMock(return_value=fake_session),
    )

    fake_thread = MagicMock()
    monkeypatch.setattr(
        threading,
        "Thread",
        MagicMock(return_value=fake_thread),
    )

    sys.modules.pop("app", None)

    module = importlib.import_module("app")
    module.app.config.update(TESTING=True)

    module._test_fake_sqs = fake_sqs
    module._test_fake_dynamodb = fake_dynamodb
    module._test_fake_thread = fake_thread

    return module


def test_health_returns_ok(app_module):
    client = app_module.app.test_client()

    response = client.get("/health")

    assert response.status_code == 200
    assert response.get_json() == {"status": "ok"}


def test_process_message_saves_event_and_deletes_sqs_message(app_module):
    message = {
        "MessageId": "message-123",
        "ReceiptHandle": "receipt-123",
        "Body": (
            '{"user_id":"user-123",'
            '"flag_name":"checkout-v2",'
            '"result":true,'
            '"timestamp":"2026-09-04T19:00:00Z"}'
        ),
    }

    app_module.process_message(message)

    app_module._test_fake_dynamodb.put_item.assert_called_once()

    put_call = app_module._test_fake_dynamodb.put_item.call_args.kwargs

    assert put_call["TableName"] == "ToggleMasterAnalytics"
    assert put_call["Item"]["user_id"] == {"S": "user-123"}
    assert put_call["Item"]["flag_name"] == {"S": "checkout-v2"}
    assert put_call["Item"]["result"] == {"BOOL": True}
    assert put_call["Item"]["timestamp"] == {
        "S": "2026-09-04T19:00:00Z"
    }
    assert "event_id" in put_call["Item"]

    app_module._test_fake_sqs.delete_message.assert_called_once_with(
        QueueUrl=app_module.SQS_QUEUE_URL,
        ReceiptHandle="receipt-123",
    )


def test_invalid_json_is_not_saved_or_deleted(app_module):
    message = {
        "MessageId": "message-invalid",
        "ReceiptHandle": "receipt-invalid",
        "Body": "{invalid-json",
    }

    app_module.process_message(message)

    app_module._test_fake_dynamodb.put_item.assert_not_called()
    app_module._test_fake_sqs.delete_message.assert_not_called()


def test_worker_is_not_started_during_tests(app_module):
    app_module._test_fake_thread.start.assert_called_once()
