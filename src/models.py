"""
Pydantic models for the Data Broker Platform.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import uuid4

from pydantic import BaseModel, Field, field_validator


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------

class DatasetCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=120, description="Human-readable dataset name")
    description: str = Field(..., min_length=1, max_length=2000)
    schema_: Dict[str, Any] = Field(..., alias="schema", description="JSON Schema describing the data")
    price_per_query: float = Field(..., gt=0, description="Price in USD per query call")
    sample_data: List[Dict[str, Any]] = Field(
        ..., min_length=1, max_length=50, description="Representative rows (max 50)"
    )
    tags: List[str] = Field(default_factory=list, max_length=10)

    @field_validator("price_per_query")
    @classmethod
    def round_price(cls, v: float) -> float:
        return round(v, 6)

    model_config = {"populate_by_name": True}


class DatasetUpdate(BaseModel):
    description: Optional[str] = Field(None, min_length=1, max_length=2000)
    price_per_query: Optional[float] = Field(None, gt=0)
    tags: Optional[List[str]] = Field(None, max_length=10)


class DatasetRecord(BaseModel):
    """Full internal record stored in the storage layer."""
    id: str = Field(default_factory=lambda: str(uuid4()))
    name: str
    description: str
    schema_: Dict[str, Any] = Field(alias="schema")
    price_per_query: float
    sample_data: List[Dict[str, Any]]
    tags: List[str] = Field(default_factory=list)
    owner_key_prefix: str = Field("", description="First 8 chars of publisher API key for display")
    mainlayer_resource_id: Optional[str] = None
    total_queries: int = 0
    total_revenue_usd: float = 0.0
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    model_config = {"populate_by_name": True}


class DatasetSummary(BaseModel):
    """Public-facing dataset listing entry."""
    id: str
    name: str
    description: str
    price_per_query: float
    tags: List[str]
    total_queries: int
    created_at: datetime


class DatasetDetail(DatasetSummary):
    """Full public dataset info including sample data and schema."""
    schema_: Dict[str, Any] = Field(alias="schema")
    sample_data: List[Dict[str, Any]]
    mainlayer_resource_id: Optional[str]

    model_config = {"populate_by_name": True}


# ---------------------------------------------------------------------------
# Query models
# ---------------------------------------------------------------------------

class QueryRequest(BaseModel):
    payer_wallet: str = Field(..., min_length=1, description="Payer's Mainlayer wallet or account ID")
    query: str = Field(..., min_length=1, max_length=1000, description="Filter / search expression")
    limit: int = Field(default=10, ge=1, le=500)


class QueryResponse(BaseModel):
    dataset_id: str
    query: str
    rows_returned: int
    data: List[Dict[str, Any]]
    charge_usd: float
    transaction_id: Optional[str] = None


# ---------------------------------------------------------------------------
# Mainlayer API response shapes
# ---------------------------------------------------------------------------

class MainlayerResource(BaseModel):
    id: str
    name: str
    price_per_call: float
    status: str
    created_at: Optional[str] = None


class MainlayerChargeResult(BaseModel):
    success: bool
    transaction_id: Optional[str] = None
    amount_usd: float
    message: str


class MainlayerEntitlementCheck(BaseModel):
    authorized: bool
    reason: Optional[str] = None


# ---------------------------------------------------------------------------
# Generic API responses
# ---------------------------------------------------------------------------

class MessageResponse(BaseModel):
    message: str
    details: Optional[Dict[str, Any]] = None


class ErrorResponse(BaseModel):
    error: str
    details: Optional[str] = None
