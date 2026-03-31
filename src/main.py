"""
Data Broker Platform — FastAPI application.

Endpoints:
  POST   /datasets              Publish a new dataset
  GET    /datasets              Discover / list available datasets
  GET    /datasets/{id}         Get full dataset info + sample data (free)
  GET    /datasets/{id}/query   Query dataset rows (pay-per-call via Mainlayer)
  DELETE /datasets/{id}         Remove a dataset
"""
from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from src.mainlayer import MainlayerClient, MainlayerError, get_client
from src.models import (
    DatasetCreate,
    DatasetDetail,
    DatasetRecord,
    DatasetSummary,
    ErrorResponse,
    MessageResponse,
    QueryResponse,
)
from src.storage import DatasetStore, get_store

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Data Broker Platform",
    description=(
        "Sell any dataset to AI agents — powered by Mainlayer payment infrastructure. "
        "Publishers earn revenue every time an agent queries their data."
    ),
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ORIGINS", "*").split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Dependency injection
# ---------------------------------------------------------------------------

def store_dep() -> DatasetStore:
    return get_store()


def client_dep() -> MainlayerClient:
    return get_client()


def get_api_key(authorization: Optional[str] = Header(None)) -> Optional[str]:
    """Extract the raw API key from the Authorization header (if provided)."""
    if not authorization:
        return None
    parts = authorization.split(" ", 1)
    if len(parts) == 2 and parts[0].lower() == "bearer":
        return parts[1]
    return None


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _to_summary(record: DatasetRecord) -> DatasetSummary:
    return DatasetSummary(
        id=record.id,
        name=record.name,
        description=record.description,
        price_per_query=record.price_per_query,
        tags=record.tags,
        total_queries=record.total_queries,
        created_at=record.created_at,
    )


def _to_detail(record: DatasetRecord) -> DatasetDetail:
    return DatasetDetail(
        id=record.id,
        name=record.name,
        description=record.description,
        schema=record.schema_,
        price_per_query=record.price_per_query,
        sample_data=record.sample_data,
        tags=record.tags,
        total_queries=record.total_queries,
        created_at=record.created_at,
        mainlayer_resource_id=record.mainlayer_resource_id,
    )


# ---------------------------------------------------------------------------
# Exception handlers
# ---------------------------------------------------------------------------

@app.exception_handler(MainlayerError)
async def mainlayer_error_handler(_: Request, exc: MainlayerError) -> JSONResponse:
    logger.error("Mainlayer API error: %s (status=%s)", exc, exc.status_code)
    http_status = exc.status_code if 400 <= exc.status_code < 600 else 502
    return JSONResponse(
        status_code=http_status,
        content={"error": "Mainlayer API error", "details": str(exc)},
    )


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@app.get("/health", tags=["meta"])
async def health() -> Dict[str, str]:
    """Liveness probe."""
    return {"status": "ok", "service": "data-broker-platform"}


# ---- Publish dataset -------------------------------------------------------

@app.post(
    "/datasets",
    response_model=DatasetDetail,
    status_code=status.HTTP_201_CREATED,
    summary="Publish a new dataset",
    tags=["datasets"],
)
async def publish_dataset(
    body: DatasetCreate,
    api_key: Optional[str] = Depends(get_api_key),
    store: DatasetStore = Depends(store_dep),
    ml_client: MainlayerClient = Depends(client_dep),
) -> DatasetDetail:
    """
    Register a dataset and make it available for AI agents to discover and pay for.

    - Creates a Mainlayer resource for payment tracking.
    - Stores dataset metadata and sample data locally.
    - Returns the full dataset record including its assigned ID.
    """
    mainlayer_resource_id: Optional[str] = None

    async with ml_client:
        try:
            resource = await ml_client.create_resource(
                name=body.name,
                description=body.description,
                price_per_call=body.price_per_query,
                tags=body.tags,
                metadata={"publisher_key_prefix": (api_key or "")[:8]},
            )
            mainlayer_resource_id = resource.id
            logger.info("Created Mainlayer resource %s for dataset '%s'", resource.id, body.name)
        except MainlayerError as exc:
            logger.warning("Mainlayer resource creation failed (%s) — continuing without it", exc)
            # Allow operation without Mainlayer in local/test mode

    record = DatasetRecord(
        name=body.name,
        description=body.description,
        schema=body.schema_,
        price_per_query=body.price_per_query,
        sample_data=body.sample_data,
        tags=body.tags,
        owner_key_prefix=(api_key or "")[:8],
        mainlayer_resource_id=mainlayer_resource_id,
    )
    saved = await store.create(record)
    logger.info("Dataset '%s' published with id=%s", saved.name, saved.id)
    return _to_detail(saved)


# ---- List / discover datasets ----------------------------------------------

@app.get(
    "/datasets",
    response_model=Dict[str, Any],
    summary="Discover available datasets",
    tags=["datasets"],
)
async def list_datasets(
    tags: Optional[str] = Query(None, description="Comma-separated tag filter"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    store: DatasetStore = Depends(store_dep),
) -> Dict[str, Any]:
    """
    List all published datasets.

    Optionally filter by tags.  Pagination via `limit` / `offset`.
    Results are sorted by publish date (newest first).
    """
    tag_list = [t.strip() for t in tags.split(",")] if tags else None
    records, total = await store.list_all(tags=tag_list, limit=limit, offset=offset)
    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "datasets": [_to_summary(r).model_dump() for r in records],
    }


# ---- Dataset detail --------------------------------------------------------

@app.get(
    "/datasets/{dataset_id}",
    response_model=DatasetDetail,
    summary="Get dataset info and sample data (free)",
    tags=["datasets"],
)
async def get_dataset(
    dataset_id: str,
    store: DatasetStore = Depends(store_dep),
) -> DatasetDetail:
    """
    Return full metadata and a sample of the dataset — no payment required.

    Use this to evaluate a dataset before paying for full query access.
    """
    record = await store.get(dataset_id)
    if record is None:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found")
    return _to_detail(record)


# ---- Query dataset (pay-per-call) ------------------------------------------

@app.get(
    "/datasets/{dataset_id}/query",
    response_model=QueryResponse,
    summary="Query a dataset (pay-per-call)",
    tags=["datasets"],
    responses={
        402: {
            "description": "Payment Required — insufficient balance or missing payer wallet",
            "model": ErrorResponse,
        }
    },
)
async def query_dataset(
    dataset_id: str,
    payer_wallet: str = Query(..., description="Payer's Mainlayer account / wallet ID"),
    query: str = Query("*", description="Filter expression: 'field=value' or free-text search"),
    limit: int = Query(10, ge=1, le=500, description="Maximum rows to return"),
    store: DatasetStore = Depends(store_dep),
    ml_client: MainlayerClient = Depends(client_dep),
) -> QueryResponse:
    """
    Query a dataset and pay per call via Mainlayer.

    Workflow:
    1. Validate dataset exists.
    2. Check payer entitlement via Mainlayer.
    3. Execute query against stored data.
    4. Charge payer via Mainlayer.
    5. Return matching rows.

    Returns HTTP 402 if payer lacks sufficient funds.
    """
    if not payer_wallet.strip():
        raise HTTPException(
            status_code=status.HTTP_402_PAYMENT_REQUIRED,
            detail="payer_wallet is required",
        )

    record = await store.get(dataset_id)
    if record is None:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found")

    charge_amount = record.price_per_query
    transaction_id: Optional[str] = None

    # ------------------------------------------------------------------
    # Payment flow (skipped gracefully when Mainlayer key is absent)
    # ------------------------------------------------------------------
    async with ml_client:
        if record.mainlayer_resource_id and ml_client._api_key:
            # 1. Check entitlement
            try:
                entitlement = await ml_client.check_entitlement(
                    resource_id=record.mainlayer_resource_id,
                    payer_wallet=payer_wallet,
                )
                if not entitlement.authorized:
                    raise HTTPException(
                        status_code=status.HTTP_402_PAYMENT_REQUIRED,
                        detail=entitlement.reason or "Insufficient funds",
                    )

                # 2. Charge payer
                charge_result = await ml_client.charge(
                    resource_id=record.mainlayer_resource_id,
                    payer_wallet=payer_wallet,
                    amount_usd=charge_amount,
                    description=f"Query: {query[:100]}",
                )
                if not charge_result.success:
                    raise HTTPException(
                        status_code=status.HTTP_402_PAYMENT_REQUIRED,
                        detail=charge_result.message,
                    )
                transaction_id = charge_result.transaction_id
                logger.info(
                    "Charged %s $%.6f for dataset %s (tx=%s)",
                    payer_wallet,
                    charge_amount,
                    dataset_id,
                    transaction_id,
                )
            except HTTPException:
                raise
            except MainlayerError as exc:
                logger.error("Mainlayer charge failed: %s", exc)
                raise HTTPException(
                    status_code=502,
                    detail=f"Payment gateway error: {exc}",
                )
        else:
            logger.debug(
                "Mainlayer not configured — serving dataset %s without payment", dataset_id
            )

    # ------------------------------------------------------------------
    # Execute query against stored data
    # ------------------------------------------------------------------
    filtered = store.filter_data(record.sample_data, query, limit)

    # Record usage stats asynchronously (fire and forget error handling)
    await store.increment_stats(dataset_id, charge_amount)

    return QueryResponse(
        dataset_id=dataset_id,
        query=query,
        rows_returned=len(filtered),
        data=filtered,
        charge_usd=charge_amount,
        transaction_id=transaction_id,
    )


# ---- Delete dataset --------------------------------------------------------

@app.delete(
    "/datasets/{dataset_id}",
    response_model=MessageResponse,
    summary="Remove a dataset",
    tags=["datasets"],
)
async def delete_dataset(
    dataset_id: str,
    api_key: Optional[str] = Depends(get_api_key),
    store: DatasetStore = Depends(store_dep),
    ml_client: MainlayerClient = Depends(client_dep),
) -> MessageResponse:
    """
    Unpublish a dataset.

    Also deactivates the corresponding Mainlayer resource so agents can no
    longer pay to access it.
    """
    record = await store.get(dataset_id)
    if record is None:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found")

    # Remove from Mainlayer
    if record.mainlayer_resource_id and ml_client._api_key:
        async with ml_client:
            try:
                await ml_client.delete_resource(record.mainlayer_resource_id)
                logger.info("Deactivated Mainlayer resource %s", record.mainlayer_resource_id)
            except MainlayerError as exc:
                logger.warning("Could not deactivate Mainlayer resource: %s", exc)

    await store.delete(dataset_id)
    logger.info("Dataset %s deleted", dataset_id)
    return MessageResponse(
        message=f"Dataset '{record.name}' deleted",
        details={"id": dataset_id},
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "src.main:app",
        host=os.getenv("HOST", "0.0.0.0"),
        port=int(os.getenv("PORT", "8000")),
        reload=os.getenv("RELOAD", "true").lower() == "true",
        log_level=os.getenv("LOG_LEVEL", "info").lower(),
    )
