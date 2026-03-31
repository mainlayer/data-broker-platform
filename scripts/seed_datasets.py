#!/usr/bin/env python3
"""
Seed script — publish 5 example datasets to the Data Broker Platform.

Usage:
    python scripts/seed_datasets.py [--base-url http://localhost:8000] [--api-key ml_...]

By default targets http://localhost:8000 with no auth header (local dev mode).
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta
from typing import Any, Dict, List

import httpx

# ---------------------------------------------------------------------------
# Dataset definitions
# ---------------------------------------------------------------------------

DATASETS: List[Dict[str, Any]] = [
    # 1. Real estate listings
    {
        "name": "US Real Estate Listings — Q1 2025",
        "description": (
            "Current residential property listings across 50 major US metro areas. "
            "Includes price, square footage, bedrooms, bathrooms, days on market, "
            "and school-district ratings. Updated daily. Ideal for price-prediction "
            "models, neighbourhood trend analysis, and investment scoring agents."
        ),
        "schema": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "listing_id": {"type": "string"},
                    "city": {"type": "string"},
                    "state": {"type": "string"},
                    "zip_code": {"type": "string"},
                    "price_usd": {"type": "number"},
                    "sqft": {"type": "integer"},
                    "bedrooms": {"type": "integer"},
                    "bathrooms": {"type": "number"},
                    "days_on_market": {"type": "integer"},
                    "school_rating": {"type": "number"},
                    "listed_at": {"type": "string", "format": "date"},
                },
                "required": ["listing_id", "city", "state", "price_usd"],
            },
        },
        "price_per_query": 0.005,
        "tags": ["real-estate", "housing", "us", "property"],
        "sample_data": [
            {
                "listing_id": "RE-10421",
                "city": "Austin",
                "state": "TX",
                "zip_code": "78701",
                "price_usd": 485000,
                "sqft": 1850,
                "bedrooms": 3,
                "bathrooms": 2.5,
                "days_on_market": 12,
                "school_rating": 8.4,
                "listed_at": "2025-01-10",
            },
            {
                "listing_id": "RE-10422",
                "city": "Miami",
                "state": "FL",
                "zip_code": "33101",
                "price_usd": 620000,
                "sqft": 2100,
                "bedrooms": 4,
                "bathrooms": 3.0,
                "days_on_market": 7,
                "school_rating": 7.1,
                "listed_at": "2025-01-15",
            },
            {
                "listing_id": "RE-10423",
                "city": "Seattle",
                "state": "WA",
                "zip_code": "98101",
                "price_usd": 795000,
                "sqft": 2450,
                "bedrooms": 4,
                "bathrooms": 3.5,
                "days_on_market": 3,
                "school_rating": 9.2,
                "listed_at": "2025-01-18",
            },
        ],
    },

    # 2. Job market data
    {
        "name": "Global Tech Job Market — Live Feed",
        "description": (
            "Real-time aggregation of software engineering, data science, and AI/ML "
            "job postings from 300+ companies worldwide. Fields include role, seniority, "
            "tech stack, remote eligibility, salary band, and application deadline. "
            "Perfect for career-guidance agents, salary benchmarking, and skills gap analysis."
        ),
        "schema": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "job_id": {"type": "string"},
                    "title": {"type": "string"},
                    "company": {"type": "string"},
                    "location": {"type": "string"},
                    "remote": {"type": "boolean"},
                    "seniority": {"type": "string", "enum": ["junior", "mid", "senior", "staff", "principal"]},
                    "tech_stack": {"type": "array", "items": {"type": "string"}},
                    "salary_min_usd": {"type": "integer"},
                    "salary_max_usd": {"type": "integer"},
                    "posted_at": {"type": "string", "format": "date"},
                    "deadline": {"type": "string", "format": "date"},
                },
            },
        },
        "price_per_query": 0.003,
        "tags": ["jobs", "employment", "tech", "salaries", "global"],
        "sample_data": [
            {
                "job_id": "JB-55801",
                "title": "Senior ML Engineer",
                "company": "Acme AI",
                "location": "San Francisco, CA",
                "remote": True,
                "seniority": "senior",
                "tech_stack": ["Python", "PyTorch", "Kubernetes", "AWS"],
                "salary_min_usd": 160000,
                "salary_max_usd": 220000,
                "posted_at": "2025-01-20",
                "deadline": "2025-02-20",
            },
            {
                "job_id": "JB-55802",
                "title": "Backend Engineer",
                "company": "DataStream Inc",
                "location": "London, UK",
                "remote": False,
                "seniority": "mid",
                "tech_stack": ["Go", "PostgreSQL", "gRPC", "GCP"],
                "salary_min_usd": 90000,
                "salary_max_usd": 130000,
                "posted_at": "2025-01-22",
                "deadline": "2025-02-15",
            },
            {
                "job_id": "JB-55803",
                "title": "Principal Data Scientist",
                "company": "FinSight Corp",
                "location": "New York, NY",
                "remote": True,
                "seniority": "principal",
                "tech_stack": ["Python", "Spark", "dbt", "Snowflake"],
                "salary_min_usd": 200000,
                "salary_max_usd": 280000,
                "posted_at": "2025-01-23",
                "deadline": "2025-02-28",
            },
        ],
    },

    # 3. Social media trends
    {
        "name": "Social Trend Signals — Weekly Pulse",
        "description": (
            "Aggregated engagement metrics and emerging topic signals from public social "
            "media across English, Spanish, and Mandarin content. Includes hashtag velocity, "
            "sentiment polarity, geographic hotspots, and virality scores. "
            "Designed for brand-monitoring agents, trend-forecasting models, and "
            "content-strategy advisors."
        ),
        "schema": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "trend_id": {"type": "string"},
                    "topic": {"type": "string"},
                    "hashtags": {"type": "array", "items": {"type": "string"}},
                    "language": {"type": "string"},
                    "sentiment": {"type": "number", "minimum": -1, "maximum": 1},
                    "virality_score": {"type": "number", "minimum": 0, "maximum": 100},
                    "weekly_mentions": {"type": "integer"},
                    "top_regions": {"type": "array", "items": {"type": "string"}},
                    "week_starting": {"type": "string", "format": "date"},
                },
            },
        },
        "price_per_query": 0.004,
        "tags": ["social-media", "trends", "sentiment", "marketing"],
        "sample_data": [
            {
                "trend_id": "TR-7801",
                "topic": "AI Agent Automation",
                "hashtags": ["#AIAgents", "#Automation", "#FutureOfWork"],
                "language": "en",
                "sentiment": 0.72,
                "virality_score": 87.4,
                "weekly_mentions": 1_420_000,
                "top_regions": ["US", "UK", "CA", "AU"],
                "week_starting": "2025-01-13",
            },
            {
                "trend_id": "TR-7802",
                "topic": "Remote Work Debate",
                "hashtags": ["#ReturnToOffice", "#WFH", "#HybridWork"],
                "language": "en",
                "sentiment": -0.18,
                "virality_score": 63.1,
                "weekly_mentions": 890_000,
                "top_regions": ["US", "DE", "FR"],
                "week_starting": "2025-01-13",
            },
            {
                "trend_id": "TR-7803",
                "topic": "Sustainable Fashion",
                "hashtags": ["#SlowFashion", "#ThriftFlip", "#EcoStyle"],
                "language": "en",
                "sentiment": 0.84,
                "virality_score": 71.9,
                "weekly_mentions": 620_000,
                "top_regions": ["SE", "NO", "NL", "UK"],
                "week_starting": "2025-01-13",
            },
        ],
    },

    # 4. Weather / climate data
    {
        "name": "Global Weather Station Data — Hourly",
        "description": (
            "Hourly observations from 12,000+ WMO weather stations worldwide. "
            "Temperature, precipitation, humidity, wind speed/direction, atmospheric "
            "pressure, UV index, and visibility. Historical data from 2010–present "
            "plus 72-hour forecasts. Invaluable for agriculture agents, logistics "
            "optimizers, and energy-demand forecasters."
        ),
        "schema": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "station_id": {"type": "string"},
                    "station_name": {"type": "string"},
                    "country": {"type": "string"},
                    "latitude": {"type": "number"},
                    "longitude": {"type": "number"},
                    "timestamp_utc": {"type": "string", "format": "date-time"},
                    "temp_c": {"type": "number"},
                    "humidity_pct": {"type": "number"},
                    "wind_speed_kmh": {"type": "number"},
                    "wind_direction_deg": {"type": "integer"},
                    "precipitation_mm": {"type": "number"},
                    "pressure_hpa": {"type": "number"},
                    "uv_index": {"type": "number"},
                    "visibility_km": {"type": "number"},
                },
            },
        },
        "price_per_query": 0.002,
        "tags": ["weather", "climate", "environment", "iot", "global"],
        "sample_data": [
            {
                "station_id": "WMO-72503",
                "station_name": "New York JFK",
                "country": "US",
                "latitude": 40.6413,
                "longitude": -73.7781,
                "timestamp_utc": "2025-01-24T12:00:00Z",
                "temp_c": 2.1,
                "humidity_pct": 68.0,
                "wind_speed_kmh": 22.5,
                "wind_direction_deg": 320,
                "precipitation_mm": 0.0,
                "pressure_hpa": 1018.4,
                "uv_index": 1.2,
                "visibility_km": 16.1,
            },
            {
                "station_id": "WMO-03772",
                "station_name": "London Heathrow",
                "country": "GB",
                "latitude": 51.4775,
                "longitude": -0.4614,
                "timestamp_utc": "2025-01-24T12:00:00Z",
                "temp_c": 8.4,
                "humidity_pct": 82.0,
                "wind_speed_kmh": 31.0,
                "wind_direction_deg": 210,
                "precipitation_mm": 1.2,
                "pressure_hpa": 1004.7,
                "uv_index": 0.5,
                "visibility_km": 9.3,
            },
            {
                "station_id": "WMO-47662",
                "station_name": "Tokyo Haneda",
                "country": "JP",
                "latitude": 35.5494,
                "longitude": 139.7798,
                "timestamp_utc": "2025-01-24T12:00:00Z",
                "temp_c": 9.7,
                "humidity_pct": 55.0,
                "wind_speed_kmh": 14.0,
                "wind_direction_deg": 350,
                "precipitation_mm": 0.0,
                "pressure_hpa": 1022.1,
                "uv_index": 2.8,
                "visibility_km": 20.0,
            },
        ],
    },

    # 5. Crypto price data
    {
        "name": "Digital Asset Price Feed — OHLCV",
        "description": (
            "Minute-level OHLCV (open, high, low, close, volume) price data for 500+ "
            "digital assets across 15 major exchanges. Includes 24h change, market cap rank, "
            "order-book depth snapshot, and funding rate for perpetual contracts. "
            "Aggregated and normalized. Essential for trading agents, portfolio rebalancers, "
            "and risk-management systems."
        ),
        "schema": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "symbol": {"type": "string"},
                    "exchange": {"type": "string"},
                    "timestamp_utc": {"type": "string", "format": "date-time"},
                    "open_usd": {"type": "number"},
                    "high_usd": {"type": "number"},
                    "low_usd": {"type": "number"},
                    "close_usd": {"type": "number"},
                    "volume_24h_usd": {"type": "number"},
                    "market_cap_usd": {"type": "number"},
                    "change_24h_pct": {"type": "number"},
                    "market_cap_rank": {"type": "integer"},
                    "funding_rate": {"type": "number"},
                },
            },
        },
        "price_per_query": 0.008,
        "tags": ["finance", "prices", "trading", "market-data"],
        "sample_data": [
            {
                "symbol": "BTC",
                "exchange": "aggregated",
                "timestamp_utc": "2025-01-24T12:00:00Z",
                "open_usd": 102_450.0,
                "high_usd": 103_900.0,
                "low_usd": 101_800.0,
                "close_usd": 103_200.0,
                "volume_24h_usd": 48_300_000_000.0,
                "market_cap_usd": 2_040_000_000_000.0,
                "change_24h_pct": 1.48,
                "market_cap_rank": 1,
                "funding_rate": 0.00012,
            },
            {
                "symbol": "ETH",
                "exchange": "aggregated",
                "timestamp_utc": "2025-01-24T12:00:00Z",
                "open_usd": 3_280.0,
                "high_usd": 3_350.0,
                "low_usd": 3_210.0,
                "close_usd": 3_315.0,
                "volume_24h_usd": 18_700_000_000.0,
                "market_cap_usd": 399_000_000_000.0,
                "change_24h_pct": 1.07,
                "market_cap_rank": 2,
                "funding_rate": 0.00008,
            },
            {
                "symbol": "SOL",
                "exchange": "aggregated",
                "timestamp_utc": "2025-01-24T12:00:00Z",
                "open_usd": 188.4,
                "high_usd": 195.0,
                "low_usd": 186.1,
                "close_usd": 193.7,
                "volume_24h_usd": 4_200_000_000.0,
                "market_cap_usd": 91_000_000_000.0,
                "change_24h_pct": 2.81,
                "market_cap_rank": 5,
                "funding_rate": 0.00021,
            },
        ],
    },
]


# ---------------------------------------------------------------------------
# Seed logic
# ---------------------------------------------------------------------------

def seed(base_url: str, api_key: str) -> None:
    headers: Dict[str, str] = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    with httpx.Client(base_url=base_url, headers=headers, timeout=30) as client:
        print(f"Seeding {len(DATASETS)} datasets to {base_url} ...\n")
        success = 0
        for ds in DATASETS:
            try:
                resp = client.post("/datasets", json=ds)
                resp.raise_for_status()
                data = resp.json()
                print(f"  [OK] {data['name']}")
                print(f"       id={data['id']}  price=${data['price_per_query']:.4f}/query")
                success += 1
            except httpx.HTTPStatusError as exc:
                print(f"  [FAIL] {ds['name']}: HTTP {exc.response.status_code} — {exc.response.text[:200]}")
            except Exception as exc:  # noqa: BLE001
                print(f"  [FAIL] {ds['name']}: {exc}")

        print(f"\nSeeding complete: {success}/{len(DATASETS)} datasets published.")
        if success < len(DATASETS):
            sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed the Data Broker Platform with example datasets")
    parser.add_argument("--base-url", default="http://localhost:8000", help="API base URL")
    parser.add_argument("--api-key", default="", help="Publisher API key (Bearer token)")
    args = parser.parse_args()
    seed(base_url=args.base_url, api_key=args.api_key)


if __name__ == "__main__":
    main()
