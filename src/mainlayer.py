"""
Mainlayer API client.

Handles resource creation, payment charging, and entitlement verification
against the Mainlayer payment-infrastructure API (https://api.mainlayer.fr).
"""
from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional

import httpx

from src.models import (
    MainlayerChargeResult,
    MainlayerEntitlementCheck,
    MainlayerResource,
)

logger = logging.getLogger(__name__)

_BASE_URL = os.getenv("MAINLAYER_BASE_URL", "https://api.mainlayer.fr")
_TIMEOUT = float(os.getenv("MAINLAYER_TIMEOUT_SECONDS", "10"))


class MainlayerError(Exception):
    """Raised when the Mainlayer API returns an unexpected error."""

    def __init__(self, message: str, status_code: int = 0, body: str = "") -> None:
        super().__init__(message)
        self.status_code = status_code
        self.body = body


class MainlayerClient:
    """
    Thin async HTTP client for the Mainlayer payment API.

    Usage:
        async with MainlayerClient(api_key="ml_...") as client:
            resource = await client.create_resource(name="My Dataset", price_per_call=0.01)
    """

    def __init__(self, api_key: Optional[str] = None) -> None:
        self._api_key = api_key or os.getenv("MAINLAYER_API_KEY", "")
        if not self._api_key:
            logger.warning("MAINLAYER_API_KEY is not set — API calls will fail with 401")
        self._client: Optional[httpx.AsyncClient] = None

    # ------------------------------------------------------------------
    # Context manager helpers
    # ------------------------------------------------------------------

    async def __aenter__(self) -> "MainlayerClient":
        self._client = httpx.AsyncClient(
            base_url=_BASE_URL,
            headers={
                "Authorization": f"Bearer {self._api_key}",
                "Content-Type": "application/json",
                "Accept": "application/json",
                "User-Agent": "data-broker-platform/1.0",
            },
            timeout=_TIMEOUT,
        )
        return self

    async def __aexit__(self, *_: Any) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _ensure_client(self) -> httpx.AsyncClient:
        if self._client is None:
            raise RuntimeError("MainlayerClient must be used as an async context manager")
        return self._client

    async def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        client = self._ensure_client()
        try:
            resp = await client.get(path, params=params)
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPStatusError as exc:
            raise MainlayerError(
                f"GET {path} failed: {exc.response.status_code}",
                status_code=exc.response.status_code,
                body=exc.response.text,
            ) from exc
        except httpx.RequestError as exc:
            raise MainlayerError(f"Network error on GET {path}: {exc}") from exc

    async def _post(self, path: str, body: Dict[str, Any]) -> Any:
        client = self._ensure_client()
        try:
            resp = await client.post(path, json=body)
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPStatusError as exc:
            raise MainlayerError(
                f"POST {path} failed: {exc.response.status_code}",
                status_code=exc.response.status_code,
                body=exc.response.text,
            ) from exc
        except httpx.RequestError as exc:
            raise MainlayerError(f"Network error on POST {path}: {exc}") from exc

    async def _delete(self, path: str) -> None:
        client = self._ensure_client()
        try:
            resp = await client.delete(path)
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise MainlayerError(
                f"DELETE {path} failed: {exc.response.status_code}",
                status_code=exc.response.status_code,
                body=exc.response.text,
            ) from exc
        except httpx.RequestError as exc:
            raise MainlayerError(f"Network error on DELETE {path}: {exc}") from exc

    # ------------------------------------------------------------------
    # Resource management
    # ------------------------------------------------------------------

    async def create_resource(
        self,
        name: str,
        description: str,
        price_per_call: float,
        tags: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> MainlayerResource:
        """
        Register a new payable resource on Mainlayer.

        Returns the created resource record including its assigned ID.
        """
        payload: Dict[str, Any] = {
            "name": name,
            "description": description,
            "price_per_call": price_per_call,
            "type": "dataset",
        }
        if tags:
            payload["tags"] = tags
        if metadata:
            payload["metadata"] = metadata

        data = await self._post("/v1/resources", payload)
        return MainlayerResource(
            id=data["id"],
            name=data.get("name", name),
            price_per_call=data.get("price_per_call", price_per_call),
            status=data.get("status", "active"),
            created_at=data.get("created_at"),
        )

    async def delete_resource(self, resource_id: str) -> None:
        """Deactivate / remove a resource from Mainlayer."""
        await self._delete(f"/v1/resources/{resource_id}")

    async def list_resources(
        self,
        limit: int = 100,
        offset: int = 0,
        tags: Optional[List[str]] = None,
    ) -> List[MainlayerResource]:
        """
        Discover available resources via the Mainlayer /discover endpoint.

        Maps to GET /v1/discover with optional tag filtering.
        """
        params: Dict[str, Any] = {"limit": limit, "offset": offset}
        if tags:
            params["tags"] = ",".join(tags)

        data = await self._get("/v1/discover", params=params)
        resources: List[MainlayerResource] = []
        items = data if isinstance(data, list) else data.get("resources", data.get("items", []))
        for item in items:
            resources.append(
                MainlayerResource(
                    id=item["id"],
                    name=item.get("name", ""),
                    price_per_call=item.get("price_per_call", 0.0),
                    status=item.get("status", "active"),
                    created_at=item.get("created_at"),
                )
            )
        return resources

    # ------------------------------------------------------------------
    # Payment / entitlement
    # ------------------------------------------------------------------

    async def check_entitlement(
        self,
        resource_id: str,
        payer_wallet: str,
    ) -> MainlayerEntitlementCheck:
        """
        Verify that a payer has sufficient balance / entitlement for a resource.

        Returns an entitlement result indicating whether the payment should proceed.
        """
        try:
            data = await self._get(
                f"/v1/resources/{resource_id}/entitlement",
                params={"payer": payer_wallet},
            )
            return MainlayerEntitlementCheck(
                authorized=bool(data.get("authorized", data.get("entitled", False))),
                reason=data.get("reason"),
            )
        except MainlayerError as exc:
            if exc.status_code == 402:
                return MainlayerEntitlementCheck(authorized=False, reason="Insufficient funds")
            raise

    async def charge(
        self,
        resource_id: str,
        payer_wallet: str,
        amount_usd: float,
        description: str = "",
    ) -> MainlayerChargeResult:
        """
        Charge the payer for a resource access event.

        Returns a charge result with transaction ID on success.
        """
        payload = {
            "resource_id": resource_id,
            "payer": payer_wallet,
            "amount": amount_usd,
            "description": description or f"Dataset query — resource {resource_id}",
        }
        try:
            data = await self._post("/v1/charges", payload)
            return MainlayerChargeResult(
                success=bool(data.get("success", data.get("status") == "completed")),
                transaction_id=data.get("transaction_id") or data.get("id"),
                amount_usd=data.get("amount", amount_usd),
                message=data.get("message", "Charge processed"),
            )
        except MainlayerError as exc:
            if exc.status_code == 402:
                return MainlayerChargeResult(
                    success=False,
                    amount_usd=amount_usd,
                    message="Payment required — insufficient funds",
                )
            raise


# ---------------------------------------------------------------------------
# Module-level singleton factory
# ---------------------------------------------------------------------------

def get_client() -> MainlayerClient:
    """Return a new MainlayerClient configured from environment variables."""
    return MainlayerClient()
