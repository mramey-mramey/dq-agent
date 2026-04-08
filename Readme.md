# DQ Agent

AI-powered data quality and cleansing agent for FP&A pipelines.  
Built with Claude (Anthropic), FastAPI, and Streamlit. Deloitte GPS practice.

---

## Architecture

```
┌─────────────────────────────────┐     ┌──────────────────────────────────┐
│  Streamlit Cloud                │     │  Railway                         │
│  frontend/app.py                │────▶│  backend/main.py (FastAPI)       │
│  + 5 pages                      │     │  backend/agent.py (Claude)       │
│                                 │     │  backend/tools/ (DQ engine)      │
└─────────────────────────────────┘     └──────────────────────────────────┘
```

**Two deployments, one repo.**  
The backend runs on Railway (FastAPI + Uvicorn).  
The frontend runs on Streamlit Cloud (Streamlit multipage app).  
They communicate over HTTPS via the `BACKEND_URL` environment variable.

---

## Prerequisites

Before you begin:

- [ ] Python 3.11 or 3.12 installed locally
- [ ] Git installed
- [ ] GitHub account (free)
- [ ] Anthropic API key — get one at [console.anthropic.com](https://console.anthropic.com)
- [ ] Railway account — sign up at [railway.app](https://railway.app) (free tier available)
- [ ] Streamlit Cloud account — sign up at [share.streamlit.io](https://share.streamlit.io) (free)

---

## Step 1 — Clone and set up locally

```bash
# 1a. Clone the repo
git clone https://github.com/YOUR_ORG/dq-agent.git
cd dq-agent

# 1b. Create and activate a virtual environment
python3 -m venv venv
source venv/bin/activate          # macOS / Linux
# venv\Scripts\activate           # Windows

# 1c. Install dependencies
pip install -r requirements.txt

# 1d. Copy the env template
cp .env.example .env
```

Open `.env` in your editor and set at minimum:

```bash
ANTHROPIC_API_KEY=sk-ant-api03-...   # Required
BACKEND_URL=http://localhost:8000    # For local dev
```

---

## Step 2 — Run locally

Open **two terminals** from the project root.

**Terminal 1 — Backend (FastAPI)**

```bash
source venv/bin/activate
uvicorn backend.main:app --reload --port 8000
```

You should see:
```
INFO:     Uvicorn running on http://0.0.0.0:8000 (Press CTRL+C to quit)
```

Verify it's healthy:
```bash
curl http://localhost:8000/health
# {"status":"ok","timestamp":"..."}
```

**Terminal 2 — Frontend (Streamlit)**

```bash
source venv/bin/activate
cd frontend
streamlit run app.py
```

Streamlit will open `http://localhost:8501` in your browser.

**Run the test suite** (optional but recommended):

```bash
# From project root, with backend NOT running (tests use TestClient)
PYTHONPATH=. pytest tests/ -v
# Expected: 413 passed
```

---

## Step 3 — Push to GitHub

```bash
# 3a. Create a new GitHub repository
# Go to github.com → New repository → name it "dq-agent" → Create

# 3b. Push your code
git init                              # if not already a git repo
git add .
git commit -m "Initial commit — DQ Agent"
git branch -M main
git remote add origin https://github.com/YOUR_ORG/dq-agent.git
git push -u origin main
```

> **Important:** Confirm `.env` is listed in `.gitignore` before pushing.
> Your API key must never appear in the repository.

---

## Step 4 — Deploy the backend to Railway

### 4a. Create a new Railway project

1. Go to [railway.app/new](https://railway.app/new)
2. Click **Deploy from GitHub repo**
3. Authorize Railway to access your GitHub account if prompted
4. Select your `dq-agent` repository
5. Railway auto-detects the `Procfile` and starts a build

### 4b. Set environment variables

In your Railway project → **Variables** tab, add:

| Variable | Value |
|---|---|
| `ANTHROPIC_API_KEY` | `sk-ant-api03-...` |
| `UPLOAD_DIR` | `/tmp/dq_uploads` |
| `OUTPUT_DIR` | `/tmp/dq_outputs` |
| `DQ_MAX_RECORDS_PER_RUN` | `50000` |
| `DQ_CONFIDENCE_THRESHOLD` | `0.80` |
| `DQ_BULK_APPROVE_THRESHOLD` | `0.95` |
| `DQ_OUTLIER_SIGMA` | `3.0` |

Railway automatically injects `PORT` — the `Procfile` already uses `$PORT`.

### 4c. Confirm the backend is live

Once the build succeeds, Railway provides a URL like:
```
https://dq-agent-production.up.railway.app
```

Test it:
```bash
curl https://dq-agent-production.up.railway.app/health
# {"status":"ok","timestamp":"..."}
```

Also open `https://dq-agent-production.up.railway.app/docs` to see the
interactive FastAPI Swagger UI — useful for troubleshooting.

> **Note on file persistence:** Railway's filesystem is ephemeral — exported
> files in `/tmp/dq_outputs` do not persist across deploys or restarts.
> Users must download their clean files immediately after export.
> For persistent storage, add a Railway Volume or use S3/Azure Blob.

---

## Step 5 — Deploy the frontend to Streamlit Cloud

### 5a. Go to Streamlit Cloud

1. Open [share.streamlit.io](https://share.streamlit.io)
2. Sign in with GitHub
3. Click **New app**

### 5b. Configure the deployment

| Field | Value |
|---|---|
| **Repository** | `YOUR_ORG/dq-agent` |
| **Branch** | `main` |
| **Main file path** | `frontend/app.py` |
| **App URL** | Choose a subdomain, e.g. `dq-agent-yourorg` |

### 5c. Set secrets

In the Streamlit Cloud app settings → **Secrets**, paste:

```toml
BACKEND_URL = "https://dq-agent-production.up.railway.app"
DEFAULT_ROLE = "analyst"
DEFAULT_USER = "analyst@yourorg.com"
```

> Streamlit secrets are loaded as environment variables at runtime.
> The `BACKEND_URL` here must match the Railway URL from Step 4c.

### 5d. Deploy

Click **Deploy!** Streamlit installs `requirements.txt` automatically.
The build takes 2–4 minutes. Your app will be live at:
```
https://dq-agent-yourorg.streamlit.app
```

---

## Step 6 — Smoke test the full stack

With both services running, run through this checklist:

- [ ] Open the Streamlit app → Home page loads with workflow steps
- [ ] Navigate to **01 · Ingest** → upload `tests/fixtures/sample_vendor_data.csv`
- [ ] Agent runs quality checks → summary appears in chat
- [ ] Navigate to **02 · Review** → issue cards load
- [ ] Approve one MEDIUM issue
- [ ] Click **Execute All Approved** → success message
- [ ] Navigate to **04 · Scorecard** → score < 100 (issues were found)
- [ ] Navigate to **03 · Export** → click Export Clean File → download button appears
- [ ] Navigate to **05 · Audit Log** → entries visible, CSV download works

---

## Environment variable reference

| Variable | Required | Default | Description |
|---|---|---|---|
| `ANTHROPIC_API_KEY` | **Yes** | — | Anthropic API key |
| `BACKEND_URL` | **Yes (frontend)** | `http://localhost:8000` | URL Streamlit uses to call FastAPI |
| `UPLOAD_DIR` | No | `/tmp/dq_uploads` | Temp dir for uploaded files |
| `OUTPUT_DIR` | No | `/tmp/dq_outputs` | Dir for exported clean files |
| `DQ_MAX_RECORDS_PER_RUN` | No | `50000` | Row limit per ingest |
| `DQ_CONFIDENCE_THRESHOLD` | No | `0.80` | Min confidence for auto-proposals |
| `DQ_BULK_APPROVE_THRESHOLD` | No | `0.95` | Min confidence for bulk approve |
| `DQ_OUTLIER_SIGMA` | No | `3.0` | Std dev threshold for outlier detection |
| `DEFAULT_ROLE` | No | `analyst` | Default UI role before user changes it |
| `DEFAULT_USER` | No | `analyst@yourorg.com` | Default username for audit log |

---

## Project structure

```
dq-agent/
├── CLAUDE.md                    # Agent spec (source of truth)
├── README.md                    # This file
├── Procfile                     # Railway: uvicorn start command
├── railway.toml                 # Railway build config
├── requirements.txt             # Pinned Python dependencies
├── .env.example                 # Env var template (copy to .env)
├── .gitignore
├── .streamlit/
│   ├── config.toml              # Streamlit theme + server config
│   └── secrets.toml.example     # Streamlit Cloud secrets template
│
├── backend/
│   ├── main.py                  # FastAPI app, all HTTP routes
│   ├── agent.py                 # Claude API orchestration + tool dispatch
│   ├── models/
│   │   ├── dataset.py           # DatasetMeta model
│   │   └── issue.py             # Issue model + lifecycle enums
│   └── tools/
│       ├── ingest.py            # File + DB ingestion
│       ├── quality_checks.py    # 7-category DQ rule engine
│       ├── entity_resolution.py # Multi-signal fuzzy entity matching
│       ├── cleanse.py           # Approved-only execution + audit log
│       └── export.py            # Clean file + DB table writer
│
├── frontend/
│   ├── app.py                   # Home page + sidebar nav
│   ├── components/
│   │   └── ui.py                # Brand CSS, API client, shared components
│   └── pages/
│       ├── 01_ingest.py         # File upload / DB connect + agent chat
│       ├── 02_review.py         # Issue cards, approve/reject, bulk actions
│       ├── 03_export.py         # Export to file or DB table
│       ├── 04_scorecard.py      # DQ score gauge + category breakdown
│       └── 05_audit_log.py      # Immutable audit trail + CSV export
│
└── tests/
    ├── fixtures/
    │   └── sample_vendor_data.csv
    ├── test_ingest.py            # 40 tests
    ├── test_quality_checks.py    # 64 tests
    ├── test_entity_resolution.py # 80 tests
    ├── test_cleanse.py           # 65 tests
    ├── test_export.py            # 47 tests
    ├── test_agent.py             # 66 tests
    └── test_main.py              # 54 tests   (413 total)
```

---

## Common issues

**"Connection refused" when Streamlit tries to reach the backend**

The `BACKEND_URL` in Streamlit secrets doesn't match the Railway URL.
Check Railway → your service → **Settings** → **Domains** for the exact URL.
Make sure it starts with `https://`, not `http://`.

**Railway build fails with "ModuleNotFoundError"**

Railway builds from the repo root. The `Procfile` runs
`uvicorn backend.main:app` — `backend` is a package relative to root.
Confirm `backend/__init__.py` exists (it should be empty, but must exist).

**Streamlit "DLL load failed" or import error on Windows locally**

Install `pyodbc` and/or `psycopg2-binary` separately if you need those DB drivers.
The base `requirements.txt` omits them to avoid platform-specific failures.

**Agent chat returns "max_iterations_reached"**

The agent hit the 10-tool-call ceiling in one turn. This typically happens
when asking it to do too much in one message. Break it into two turns:
first "run quality checks", then "run entity resolution".

**Export fails with "already been exported"**

`DatasetMeta.export_ready` is set to `True` after the first export.
This is intentional — it prevents double-writes. Start a new session from
the **01 · Ingest** page if you need to re-export.

**Uploaded files disappear after Railway redeploy**

Railway's filesystem is ephemeral. Download your clean file immediately
after export. For durable storage, mount a Railway Volume at `OUTPUT_DIR`
or replace the file export with S3 / Azure Blob upload.

---

## Updating the deployment

```bash
# Make your changes locally, test, then:
git add .
git commit -m "Your change description"
git push origin main
```

Railway redeploys automatically on push to `main`.  
Streamlit Cloud redeploys automatically on push to `main`.  
Both have zero-downtime rolling restarts.

---

## Running tests

```bash
# Full suite (413 tests, ~9 seconds)
PYTHONPATH=. pytest tests/ -v

# Single module
PYTHONPATH=. pytest tests/test_quality_checks.py -v

# With coverage
pip install pytest-cov
PYTHONPATH=. pytest tests/ --cov=backend --cov-report=term-missing
```

---

*Together makes progress*