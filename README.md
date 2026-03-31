# Data Broker Platform

**Sell any dataset to AI agents — powered by Mainlayer payment infrastructure.**

Publish a dataset once. Earn revenue every time an AI agent queries it. No contracts, no invoicing, no minimum volumes — just a REST API and an API key.

---

## What is this?

The Data Broker Platform is an open monetization layer that sits between data publishers and AI agent consumers.

- **Publishers** upload datasets and set a price per query.
- **Agents** discover datasets via `/datasets`, inspect samples for free, and pay per call using their Mainlayer wallet.
- **Revenue** flows automatically through Mainlayer's payment infrastructure with no human intermediary.

---

## 5-Minute Setup

### Prerequisites

- Python 3.11+
- A [Mainlayer](https://app.mainlayer.xyz) account and API key

### 1. Clone and install

```bash
git clone https://github.com/mainlayer/data-broker-platform
cd data-broker-platform

python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Configure environment

```bash
cp .env.example .env
# Edit .env and set MAINLAYER_API_KEY=ml_your_key
```

### 3. Start the server

```bash
python -m src.main
# or
uvicorn src.main:app --reload
```

The API is now live at `http://localhost:8000`. Interactive docs at `http://localhost:8000/docs`.

### 4. Seed example datasets (optional)

```bash
python scripts/seed_datasets.py
```

This publishes 5 ready-to-use datasets: real estate listings, tech jobs, social trends, weather data, and digital asset prices.

---

## Docker

```bash
cp .env.example .env   # set MAINLAYER_API_KEY

docker compose up -d

# Check it's running
curl http://localhost:8000/health
```

---

## How Agents Discover and Pay for Data

### Step 1 — Discover

```http
GET /datasets
```

Returns all available datasets with name, description, price per query, and tags. No auth required.

```json
{
  "total": 5,
  "datasets": [
    {
      "id": "3f7a1b2c-...",
      "name": "US Real Estate Listings — Q1 2025",
      "description": "Current residential property listings across 50 US metro areas...",
      "price_per_query": 0.005,
      "tags": ["real-estate", "housing", "us"],
      "total_queries": 1420
    }
  ]
}
```

### Step 2 — Inspect (free)

```http
GET /datasets/{id}
```

Returns the full schema and sample rows at no cost, so agents can evaluate a dataset before committing to a paid query.

### Step 3 — Query (pay-per-call)

```http
GET /datasets/{id}/query?payer_wallet=wlt_agent_abc&query=city%3DAustin&limit=50
```

The platform:
1. Verifies the payer's Mainlayer balance.
2. Executes the query against the dataset.
3. Charges the payer via Mainlayer.
4. Returns matching rows.

```json
{
  "dataset_id": "3f7a1b2c-...",
  "query": "city=Austin",
  "rows_returned": 14,
  "data": [...],
  "charge_usd": 0.005,
  "transaction_id": "tx_9f3a..."
}
```

If the payer has insufficient funds, the API returns **HTTP 402 Payment Required** — a standard signal that agents can handle gracefully.

---

## Publishing a Dataset

```bash
curl -X POST http://localhost:8000/datasets \
  -H "Authorization: Bearer ml_your_publisher_key" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "My Custom Dataset",
    "description": "What this dataset contains and who should use it.",
    "schema": {
      "type": "array",
      "items": {
        "type": "object",
        "properties": {
          "id":    { "type": "integer" },
          "label": { "type": "string" }
        }
      }
    },
    "price_per_query": 0.01,
    "tags": ["custom", "example"],
    "sample_data": [
      { "id": 1, "label": "foo" },
      { "id": 2, "label": "bar" }
    ]
  }'
```

Response includes the dataset `id` and its Mainlayer `resource_id` — the handle agents use to pay for access.

---

## API Reference

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| `GET` | `/health` | None | Liveness probe |
| `POST` | `/datasets` | Bearer | Publish a dataset |
| `GET` | `/datasets` | None | List / discover datasets |
| `GET` | `/datasets/{id}` | None | Get full info + sample (free) |
| `GET` | `/datasets/{id}/query` | None* | Query dataset (charged via Mainlayer) |
| `DELETE` | `/datasets/{id}` | Bearer | Unpublish a dataset |

*The payer wallet ID is passed as a query parameter; the call itself does not require an API key.

Full interactive documentation: `http://localhost:8000/docs`

---

## Query Syntax

The `query` parameter supports two modes:

| Mode | Example | Behaviour |
|------|---------|-----------|
| Wildcard | `*` | Returns all rows up to `limit` |
| Field match | `city=Austin` | Exact match on named field |
| Free-text | `Austin` | Substring match across all values |

---

## Revenue Sharing Model

Publishers earn revenue every time an agent queries their dataset.

- You set the price — anywhere from fractions of a cent to dollars per query.
- Mainlayer processes payments and settles to your account on a rolling basis.
- No platform fee is deducted by this open-source layer; Mainlayer's standard processing fee applies (see [Mainlayer pricing](https://mainlayer.xyz/pricing)).
- Track earnings via the `total_revenue_usd` field on each dataset or in the Mainlayer dashboard.

**Example earnings:**

| Dataset | Queries/day | Price | Daily revenue |
|---------|-------------|-------|---------------|
| Real estate | 2,000 | $0.005 | $10 |
| Job market | 5,000 | $0.003 | $15 |
| Weather | 10,000 | $0.002 | $20 |

---

## Configuration

All configuration is via environment variables (see `.env.example`):

| Variable | Default | Description |
|----------|---------|-------------|
| `MAINLAYER_API_KEY` | — | Your Mainlayer publisher API key |
| `MAINLAYER_BASE_URL` | `https://api.mainlayer.xyz` | Mainlayer API endpoint |
| `PORT` | `8000` | Server port |
| `STORAGE_PATH` | _(empty)_ | JSON file for local persistence |
| `LOG_LEVEL` | `INFO` | Logging verbosity |
| `CORS_ORIGINS` | `*` | Allowed CORS origins |

---

## Running Tests

```bash
pytest tests/ -v
```

The test suite covers 20+ scenarios including publish, list, filter, query (with and without payment), deletion, input validation, and error handling. All Mainlayer API calls are mocked so tests run fully offline.

---

## Project Structure

```
data-broker-platform/
├── src/
│   ├── main.py          # FastAPI app and route handlers
│   ├── mainlayer.py     # Mainlayer API client
│   ├── storage.py       # In-memory + JSON persistence
│   └── models.py        # Pydantic models
├── scripts/
│   └── seed_datasets.py # 5 example datasets
├── tests/
│   └── test_api.py      # 20+ API tests
├── .env.example
├── docker-compose.yml
├── Dockerfile
└── requirements.txt
```

---

## Support

- Mainlayer documentation: [https://docs.mainlayer.xyz](https://docs.mainlayer.xyz)
- Issues: open a GitHub issue on this repository
