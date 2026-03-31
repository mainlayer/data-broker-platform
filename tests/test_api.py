"""
Test suite for the Data Broker Platform API.

Covers 20+ test cases across all endpoints:
  - Dataset publishing
  - Dataset listing and filtering
  - Dataset detail retrieval
  - Pay-per-query flow (with and without Mainlayer configured)
  - Dataset deletion
  - Input validation and error handling
  - Edge cases
"""
from __future__ import annotations

from typing import Any, Dict
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient

from src.main import app, client_dep, store_dep
from src.models import (
    MainlayerChargeResult,
    MainlayerEntitlementCheck,
    MainlayerResource,
)
from src.storage import DatasetStore

# ---------------------------------------------------------------------------
# Mock factories
# ---------------------------------------------------------------------------

SAMPLE_PAYLOAD: Dict[str, Any] = {
    "name": "Test Dataset",
    "description": "A dataset for testing purposes",
    "schema": {
        "type": "array",
        "items": {
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "value": {"type": "string"},
            },
        },
    },
    "price_per_query": 0.01,
    "tags": ["test", "sample"],
    "sample_data": [
        {"id": 1, "value": "alpha"},
        {"id": 2, "value": "beta"},
        {"id": 3, "value": "gamma"},
    ],
}


def _make_no_key_ml() -> AsyncMock:
    """ML client with no API key — payment path is skipped."""
    ml = AsyncMock()
    ml.__aenter__ = AsyncMock(return_value=ml)
    ml.__aexit__ = AsyncMock(return_value=False)
    ml._api_key = ""
    ml.create_resource = AsyncMock(side_effect=Exception("no key — skipped"))
    ml.delete_resource = AsyncMock(return_value=None)
    return ml


def _make_active_ml(resource_id: str = "res_abc123") -> AsyncMock:
    """ML client with a valid API key and happy-path responses."""
    ml = AsyncMock()
    ml.__aenter__ = AsyncMock(return_value=ml)
    ml.__aexit__ = AsyncMock(return_value=False)
    ml._api_key = "ml_test_key"
    ml.create_resource = AsyncMock(
        return_value=MainlayerResource(
            id=resource_id,
            name="Test Dataset",
            price_per_call=0.01,
            status="active",
        )
    )
    ml.delete_resource = AsyncMock(return_value=None)
    ml.check_entitlement = AsyncMock(
        return_value=MainlayerEntitlementCheck(authorized=True)
    )
    ml.charge = AsyncMock(
        return_value=MainlayerChargeResult(
            success=True,
            transaction_id="tx_xyz789",
            amount_usd=0.01,
            message="OK",
        )
    )
    return ml


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def store() -> DatasetStore:
    return DatasetStore()


@pytest.fixture()
def ml_no_key() -> AsyncMock:
    return _make_no_key_ml()


@pytest.fixture()
def ml_active() -> AsyncMock:
    return _make_active_ml()


@pytest.fixture()
def client(store: DatasetStore, ml_no_key: AsyncMock) -> TestClient:
    """
    Default test client: fresh store, no Mainlayer API key.
    Individual tests that need an active ML client override client_dep themselves.
    """
    app.dependency_overrides[store_dep] = lambda: store
    app.dependency_overrides[client_dep] = lambda: ml_no_key
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# Publish helper
# ---------------------------------------------------------------------------

def publish(
    client: TestClient,
    payload: Dict[str, Any] = None,
) -> Dict[str, Any]:
    resp = client.post("/datasets", json=payload or SAMPLE_PAYLOAD)
    assert resp.status_code == 201, resp.text
    return resp.json()


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

class TestHealth:
    def test_health_returns_ok(self, client: TestClient) -> None:
        resp = client.get("/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"


# ---------------------------------------------------------------------------
# POST /datasets
# ---------------------------------------------------------------------------

class TestPublishDataset:
    def test_publish_returns_201(self, client: TestClient) -> None:
        data = publish(client)
        assert data["name"] == SAMPLE_PAYLOAD["name"]

    def test_publish_returns_expected_fields(self, client: TestClient) -> None:
        data = publish(client)
        assert data["description"] == SAMPLE_PAYLOAD["description"]
        assert data["price_per_query"] == SAMPLE_PAYLOAD["price_per_query"]
        assert "id" in data
        assert "created_at" in data

    def test_publish_includes_sample_data(self, client: TestClient) -> None:
        data = publish(client)
        assert len(data["sample_data"]) == 3

    def test_publish_includes_tags(self, client: TestClient) -> None:
        data = publish(client)
        assert set(data["tags"]) == {"test", "sample"}

    def test_publish_with_mainlayer_resource_id(self, store: DatasetStore, ml_active: AsyncMock) -> None:
        app.dependency_overrides[store_dep] = lambda: store
        app.dependency_overrides[client_dep] = lambda: ml_active
        with TestClient(app) as c:
            data = publish(c)
        app.dependency_overrides.clear()
        assert data["mainlayer_resource_id"] == "res_abc123"

    def test_publish_without_mainlayer_resource_id_is_none(self, client: TestClient) -> None:
        data = publish(client)
        assert data["mainlayer_resource_id"] is None

    def test_publish_missing_name_returns_422(self, client: TestClient) -> None:
        payload = {k: v for k, v in SAMPLE_PAYLOAD.items() if k != "name"}
        resp = client.post("/datasets", json=payload)
        assert resp.status_code == 422

    def test_publish_missing_schema_returns_422(self, client: TestClient) -> None:
        payload = {k: v for k, v in SAMPLE_PAYLOAD.items() if k != "schema"}
        resp = client.post("/datasets", json=payload)
        assert resp.status_code == 422

    def test_publish_zero_price_returns_422(self, client: TestClient) -> None:
        resp = client.post("/datasets", json={**SAMPLE_PAYLOAD, "price_per_query": 0})
        assert resp.status_code == 422

    def test_publish_negative_price_returns_422(self, client: TestClient) -> None:
        resp = client.post("/datasets", json={**SAMPLE_PAYLOAD, "price_per_query": -1.0})
        assert resp.status_code == 422

    def test_publish_empty_sample_data_returns_422(self, client: TestClient) -> None:
        resp = client.post("/datasets", json={**SAMPLE_PAYLOAD, "sample_data": []})
        assert resp.status_code == 422


# ---------------------------------------------------------------------------
# GET /datasets
# ---------------------------------------------------------------------------

class TestListDatasets:
    def test_empty_list(self, client: TestClient) -> None:
        resp = client.get("/datasets")
        assert resp.status_code == 200
        assert resp.json()["total"] == 0
        assert resp.json()["datasets"] == []

    def test_list_returns_published_datasets(self, client: TestClient) -> None:
        publish(client, {**SAMPLE_PAYLOAD, "name": "Dataset A"})
        publish(client, {**SAMPLE_PAYLOAD, "name": "Dataset B"})
        data = client.get("/datasets").json()
        assert data["total"] == 2
        assert len(data["datasets"]) == 2

    def test_list_tag_filter(self, client: TestClient) -> None:
        publish(client, {**SAMPLE_PAYLOAD, "name": "Weather", "tags": ["weather"]})
        publish(client, {**SAMPLE_PAYLOAD, "name": "Jobs", "tags": ["jobs"]})
        data = client.get("/datasets?tags=weather").json()
        assert data["total"] == 1
        assert data["datasets"][0]["name"] == "Weather"

    def test_list_multi_tag_filter(self, client: TestClient) -> None:
        publish(client, {**SAMPLE_PAYLOAD, "name": "Weather", "tags": ["weather"]})
        publish(client, {**SAMPLE_PAYLOAD, "name": "Jobs", "tags": ["jobs"]})
        publish(client, {**SAMPLE_PAYLOAD, "name": "Other", "tags": ["other"]})
        assert client.get("/datasets?tags=weather,jobs").json()["total"] == 2

    def test_list_pagination_limit(self, client: TestClient) -> None:
        for i in range(5):
            publish(client, {**SAMPLE_PAYLOAD, "name": f"DS {i}"})
        data = client.get("/datasets?limit=2&offset=0").json()
        assert data["total"] == 5
        assert len(data["datasets"]) == 2

    def test_list_pagination_offset(self, client: TestClient) -> None:
        for i in range(4):
            publish(client, {**SAMPLE_PAYLOAD, "name": f"DS {i}"})
        data = client.get("/datasets?limit=10&offset=2").json()
        assert len(data["datasets"]) == 2

    def test_list_returns_summary_not_full_detail(self, client: TestClient) -> None:
        publish(client)
        item = client.get("/datasets").json()["datasets"][0]
        assert "sample_data" not in item


# ---------------------------------------------------------------------------
# GET /datasets/{id}
# ---------------------------------------------------------------------------

class TestGetDataset:
    def test_get_existing_dataset(self, client: TestClient) -> None:
        ds = publish(client)
        resp = client.get(f"/datasets/{ds['id']}")
        assert resp.status_code == 200
        assert resp.json()["id"] == ds["id"]

    def test_get_includes_sample_data(self, client: TestClient) -> None:
        ds = publish(client)
        resp = client.get(f"/datasets/{ds['id']}")
        assert len(resp.json()["sample_data"]) == 3

    def test_get_includes_schema(self, client: TestClient) -> None:
        ds = publish(client)
        resp = client.get(f"/datasets/{ds['id']}")
        assert "schema" in resp.json()

    def test_get_nonexistent_returns_404(self, client: TestClient) -> None:
        assert client.get("/datasets/nonexistent-id").status_code == 404


# ---------------------------------------------------------------------------
# GET /datasets/{id}/query
# ---------------------------------------------------------------------------

class TestQueryDataset:
    def test_query_without_mainlayer_returns_data(self, client: TestClient) -> None:
        ds = publish(client)
        resp = client.get(
            f"/datasets/{ds['id']}/query",
            params={"payer_wallet": "wallet_abc", "query": "*", "limit": 10},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["dataset_id"] == ds["id"]
        assert isinstance(body["data"], list)
        assert body["rows_returned"] > 0

    def test_query_wildcard_returns_all_sample_rows(self, client: TestClient) -> None:
        ds = publish(client)
        resp = client.get(
            f"/datasets/{ds['id']}/query",
            params={"payer_wallet": "wallet_abc", "query": "*", "limit": 100},
        )
        assert resp.json()["rows_returned"] == 3

    def test_query_with_field_filter(self, client: TestClient) -> None:
        ds = publish(client)
        resp = client.get(
            f"/datasets/{ds['id']}/query",
            params={"payer_wallet": "wallet_abc", "query": "value=alpha", "limit": 10},
        )
        body = resp.json()
        assert body["rows_returned"] == 1
        assert body["data"][0]["value"] == "alpha"

    def test_query_with_freetext_filter(self, client: TestClient) -> None:
        ds = publish(client)
        resp = client.get(
            f"/datasets/{ds['id']}/query",
            params={"payer_wallet": "wallet_abc", "query": "beta", "limit": 10},
        )
        assert resp.json()["rows_returned"] == 1

    def test_query_limit_respected(self, client: TestClient) -> None:
        ds = publish(client)
        resp = client.get(
            f"/datasets/{ds['id']}/query",
            params={"payer_wallet": "wallet_abc", "query": "*", "limit": 1},
        )
        assert resp.json()["rows_returned"] == 1

    def test_query_nonexistent_dataset_returns_404(self, client: TestClient) -> None:
        resp = client.get(
            "/datasets/bad-id/query",
            params={"payer_wallet": "wallet_abc", "query": "*"},
        )
        assert resp.status_code == 404

    def test_query_with_mainlayer_authorized(self, store: DatasetStore, ml_active: AsyncMock) -> None:
        # Publish with active ML so dataset gets a resource_id
        app.dependency_overrides[store_dep] = lambda: store
        app.dependency_overrides[client_dep] = lambda: ml_active
        with TestClient(app) as c:
            ds = publish(c)
            resp = c.get(
                f"/datasets/{ds['id']}/query",
                params={"payer_wallet": "wallet_test", "query": "*", "limit": 5},
            )
        app.dependency_overrides.clear()
        assert resp.status_code == 200
        assert resp.json()["transaction_id"] == "tx_xyz789"

    def test_query_with_mainlayer_payment_required(self, store: DatasetStore, ml_active: AsyncMock) -> None:
        app.dependency_overrides[store_dep] = lambda: store
        app.dependency_overrides[client_dep] = lambda: ml_active
        with TestClient(app) as c:
            ds = publish(c)

        # Now use a client that denies entitlement
        ml_broke = _make_active_ml()
        ml_broke.check_entitlement = AsyncMock(
            return_value=MainlayerEntitlementCheck(authorized=False, reason="Insufficient funds")
        )
        app.dependency_overrides[store_dep] = lambda: store
        app.dependency_overrides[client_dep] = lambda: ml_broke
        with TestClient(app) as c:
            resp = c.get(
                f"/datasets/{ds['id']}/query",
                params={"payer_wallet": "broke_wallet", "query": "*"},
            )
        app.dependency_overrides.clear()
        assert resp.status_code == 402

    def test_query_response_includes_charge_amount(self, client: TestClient) -> None:
        ds = publish(client)
        resp = client.get(
            f"/datasets/{ds['id']}/query",
            params={"payer_wallet": "wallet_abc", "query": "*"},
        )
        assert resp.json()["charge_usd"] == SAMPLE_PAYLOAD["price_per_query"]

    def test_query_increments_total_queries(self, client: TestClient) -> None:
        ds = publish(client)
        for _ in range(2):
            client.get(
                f"/datasets/{ds['id']}/query",
                params={"payer_wallet": "w", "query": "*"},
            )
        resp = client.get(f"/datasets/{ds['id']}")
        assert resp.json()["total_queries"] == 2

    def test_query_response_has_correct_structure(self, client: TestClient) -> None:
        ds = publish(client)
        resp = client.get(
            f"/datasets/{ds['id']}/query",
            params={"payer_wallet": "wallet_abc", "query": "*"},
        )
        body = resp.json()
        for field in ("dataset_id", "query", "rows_returned", "data", "charge_usd"):
            assert field in body


# ---------------------------------------------------------------------------
# DELETE /datasets/{id}
# ---------------------------------------------------------------------------

class TestDeleteDataset:
    def test_delete_returns_200(self, client: TestClient) -> None:
        ds = publish(client)
        assert client.delete(f"/datasets/{ds['id']}").status_code == 200

    def test_delete_removes_from_list(self, client: TestClient) -> None:
        ds = publish(client)
        client.delete(f"/datasets/{ds['id']}")
        assert client.get("/datasets").json()["total"] == 0

    def test_delete_makes_detail_unavailable(self, client: TestClient) -> None:
        ds = publish(client)
        client.delete(f"/datasets/{ds['id']}")
        assert client.get(f"/datasets/{ds['id']}").status_code == 404

    def test_delete_nonexistent_returns_404(self, client: TestClient) -> None:
        assert client.delete("/datasets/nonexistent-id").status_code == 404

    def test_delete_deactivates_mainlayer_resource(self, store: DatasetStore, ml_active: AsyncMock) -> None:
        app.dependency_overrides[store_dep] = lambda: store
        app.dependency_overrides[client_dep] = lambda: ml_active
        with TestClient(app) as c:
            ds = publish(c)
            resp = c.delete(f"/datasets/{ds['id']}")
        app.dependency_overrides.clear()
        assert resp.status_code == 200
        ml_active.delete_resource.assert_called_once_with("res_abc123")

    def test_delete_response_contains_dataset_name(self, client: TestClient) -> None:
        ds = publish(client)
        resp = client.delete(f"/datasets/{ds['id']}")
        assert SAMPLE_PAYLOAD["name"] in resp.json()["message"]
