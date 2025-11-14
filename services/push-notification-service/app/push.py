import logging
from datetime import datetime, timedelta
from typing import Optional

import requests
from google.auth.transport.requests import Request
from google.oauth2 import service_account

from app.schemas import PushNotificationPayload


logger = logging.getLogger(__name__)


class CircuitBreaker:
    """Simple circuit breaker to guard FCM calls."""

    def __init__(self, failure_threshold: int = 5, timeout: int = 60):
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.failures = 0
        self.last_failure_time: Optional[datetime] = None
        self.state = "CLOSED"

    def call(self, func, *args, **kwargs):
        if self.state == "OPEN":
            if datetime.now() - self.last_failure_time > timedelta(seconds=self.timeout):
                self.state = "HALF_OPEN"
                logger.info("Circuit breaker entering HALF_OPEN state")
            else:
                raise Exception("Circuit breaker is OPEN")

        try:
            result = func(*args, **kwargs)
            if self.state == "HALF_OPEN":
                self.state = "CLOSED"
                self.failures = 0
                logger.info("Circuit breaker reset to CLOSED state")
            return result
        except Exception as exc:
            self.failures += 1
            self.last_failure_time = datetime.now()

            if self.failures >= self.failure_threshold:
                self.state = "OPEN"
                logger.error("Circuit breaker opened after %s failures", self.failures)
            raise exc


class PushServiceManager:
    """Wrapper around Firebase Cloud Messaging HTTP v1 API."""

    def __init__(self, service_account_path: str, project_id: str):
        self.service_account_path = service_account_path
        self.project_id = project_id
        self.credentials = None
        self.circuit_breaker = CircuitBreaker(failure_threshold=5, timeout=60)
        self._init_credentials()

    def _init_credentials(self):
        try:
            self.credentials = service_account.Credentials.from_service_account_file(
                self.service_account_path,
                scopes=["https://www.googleapis.com/auth/cloud-platform"],
            )
            logger.info("Firebase credentials initialized successfully")
        except Exception as exc:
            logger.error("Failed to initialize credentials: %s", exc)
            raise

    def get_access_token(self):
        try:
            self.credentials.refresh(Request())
            return self.credentials.token
        except Exception as exc:
            logger.error("Failed to refresh access token: %s", exc)
            raise

    def send_push_notification(self, payload: PushNotificationPayload, correlation_id: str):
        def _send():
            access_token = self.get_access_token()
            url = f"https://fcm.googleapis.com/v1/projects/{self.project_id}/messages:send"

            headers = {
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json; UTF-8",
            }

            message = {
                "message": {
                    "token": payload.token,
                    "notification": {
                        "title": payload.title,
                        "body": payload.body,
                    }
                }
            }

            if payload.image:
                message["message"]["notification"]["image"] = payload.image

            if payload.link:
                message["message"]["webpush"] = {
                    "fcm_options": {
                        "link": payload.link
                    }
                }

            if payload.data:
                message["message"]["data"] = payload.data

            logger.info("[%s] Sending notification to FCM", correlation_id)
            response = requests.post(url, headers=headers, json=message, timeout=10)

            if response.status_code == 200:
                logger.info("[%s] Notification sent successfully", correlation_id)
                return response.json()

            logger.error("[%s] FCM error: %s", correlation_id, response.text)
            raise Exception(f"FCM Error: {response.text}")

        return self.circuit_breaker.call(_send)

