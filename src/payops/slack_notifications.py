"""Explicit Slack reference notifications carry no evidence, credentials or execution authority."""

import re
from urllib.parse import urlsplit

import httpx
from fastapi import APIRouter, HTTPException
from pydantic import SecretStr

from payops.memory.store import IncidentStore
from payops.protected_api import Authenticator, Bearer, actor, authorize, scoped_incident


class SlackNotifier:
    """Host configuration fixes the Slack destination; requests cannot choose arbitrary URLs."""

    def __init__(
        self, webhook: SecretStr, operator_origin: str, transport: httpx.BaseTransport | None = None
    ) -> None:
        """Require a Slack webhook and HTTPS operator origin, with no redirects or auto retries."""
        if (
            re.fullmatch(
                r"https://hooks\.slack\.com/services/[A-Za-z0-9]+/[A-Za-z0-9]+/[A-Za-z0-9]+",
                webhook.get_secret_value(),
            )
            is None
        ):
            raise ValueError("invalid Slack webhook")
        origin = urlsplit(operator_origin)
        if (
            origin.scheme != "https"
            or not origin.hostname
            or origin.username
            or origin.password
            or origin.path not in {"", "/"}
            or origin.query
            or origin.fragment
        ):
            raise ValueError("invalid operator origin")
        self._webhook, self._origin = webhook, operator_origin.rstrip("/")
        self._transport = transport

    def send(self, incident_id: str) -> None:
        """Send a fixed reference; unknown delivery is not retried and errors redact the URL."""
        if re.fullmatch(r"[A-Za-z0-9_-]{1,128}", incident_id) is None:
            raise ValueError("invalid incident reference")
        url = f"{self._origin}/api/incidents/{incident_id}"
        payload = {
            "text": f"PayOps incident {incident_id}: {url}",
            "unfurl_links": False,
            "unfurl_media": False,
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "plain_text",
                        "text": (
                            f"PayOps incident {incident_id}. Review: {url}. "
                            "Approval and execution require backend authorization."
                        ),
                    },
                }
            ],
        }
        try:
            with httpx.Client(
                timeout=5, trust_env=False, follow_redirects=False, transport=self._transport
            ) as client:
                with client.stream(
                    "POST", self._webhook.get_secret_value(), json=payload
                ) as response:
                    if response.status_code != 200:
                        raise ValueError("notification rejected")
                    body = bytearray()
                    for chunk in response.iter_bytes():
                        body.extend(chunk)
                        if len(body) > 1024:
                            raise ValueError("notification response exceeded bound")
                    if bytes(body).strip() != b"ok":
                        raise ValueError("notification rejected")
        except (httpx.HTTPError, ValueError):
            raise RuntimeError("SLACK_DELIVERY_UNCONFIRMED") from None


def slack_router(
    store: IncidentStore, identity: Authenticator, notifier: SlackNotifier
) -> APIRouter:
    """Only a scoped responder can notify the host-configured channel."""
    router = APIRouter(tags=["notifications"])

    @router.post("/api/incidents/{incident_id}/notifications/slack", status_code=202)
    def notify(incident_id: str, bearer: Bearer) -> dict[str, str]:
        """Delivery sends references only and cannot invoke an approval or remediation callback."""
        principal = actor(identity, bearer)
        item = scoped_incident(store, incident_id, principal)
        authorize(principal, item.request.namespace, write=True)
        try:
            notifier.send(item.incident_id)
        except RuntimeError:
            raise HTTPException(502, detail="SLACK_DELIVERY_UNCONFIRMED") from None
        return {"status": "delivered", "incident_id": item.incident_id}

    return router
