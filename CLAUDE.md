# CLAUDE.md — Data Quality & Cleansing Agent

## Project Overview

This is an AI-powered data quality and cleansing agent built with Claude as the reasoning layer. The agent ingests structured datasets, identifies data quality issues across multiple dimensions, proposes remediation steps, and executes approved cleansing actions — with a human-in-the-loop approval gate before any data is modified.

**Primary use case:** FP&A and operational data pipelines where incoming records must meet quality standards before being loaded into downstream systems (GL, ERP, vendor master, etc.).

---

## Architecture

Two ingestion modes are supported. Both converge at the Quality Check Engine and share the same approval and audit infrastructure. Output destination is determined by the ingestion source.

```
┌──────────────────────────┐     ┌──────────────────────────┐
│  MODE A: File Upload     │     │  MODE B: Live DB          │
│  CSV or Excel via        │     │  Connection string +      │
│  Streamlit UI            │     │  table/query selector     │
└────────────┬─────────────┘     └─────────────┬────────────┘
             │                                 │
             ▼                                 ▼
     pandas DataFrame                  pandas DataFrame
     (held in memory,                  (sampled or full,
      temp file on disk)                read-only conn)
             │                                 │
             └──────────────┬──────────────────┘
                            ▼
               ┌─────────────────────┐
               │  Quality Check      │  Claude analyzes data against rule sets
               │  Engine (Claude)    │  Returns structured DQ report (JSON)
               └────────┬────────────┘
                        │
                        ▼
               ┌─────────────────────┐
               │  Issue Registry     │  Stores flagged issues with severity,
               │  (SQLite / Postgres) │  confidence score, and proposed fix
               └────────┬────────────┘
                        │
                        ▼
               ┌─────────────────────┐
               │  Approval UI        │  Streamlit — human reviews & approves
               │  (Human-in-Loop)    │  or rejects each proposed change
               └────────┬────────────┘
                        │
                        ▼
               ┌─────────────────────┐
               │  Cleansing Engine   │  Executes only approved transformations
               │  (Claude + Tools)   │  Produces audit log of all changes
               └────────┬────────────┘
                        │
             ┌──────────┴──────────┐
             ▼                     ▼
  ┌──────────────────┐   ┌──────────────────────┐
  │  MODE A OUTPUT   │   │  MODE B OUTPUT        │
  │  New clean file  │   │  New table written    │
  │  (CSV or Excel,  │   │  to same DB schema    │
  │   download link) │   │  e.g. vendors_clean   │
  └──────────────────┘   └──────────────────────┘
```

**Stack:**
- **Backend:** Python, FastAPI
- **AI Reasoning:** Anthropic Claude API (claude-sonnet-4-20250514) with tool use
- **UI:** Streamlit
- **Deployment:** Railway (backend) + Streamlit Cloud (UI)
- **Storage:** PostgreSQL or SQLite for agent internals (issue registry, audit log); source DB connections are separate and read-only until cleanse execution
- **Auth / RBAC:** Role-based access — Analyst (view/approve), Admin (configure rules)

---

## Ingestion Layer

### Mode A — File Upload (CSV / Excel)

- User uploads a file via the Streamlit UI (`.csv`, `.xlsx`, `.xls`)
- Backend reads the file into a pandas DataFrame using `pandas.read_csv` or `pandas.read_excel`
- File is stored as a temp artifact tied to a `dataset_id` for the duration of the session
- On cleanse execution, a new clean file is written and made available as a download
  - CSV input → CSV output: `{original_filename}_clean_{timestamp}.csv`
  - Excel input → Excel output: `{original_filename}_clean_{timestamp}.xlsx`
  - Sheet structure and column order are preserved; only flagged cells are modified
- Original uploaded file is **never overwritten**

### Mode B — Live Database Connection

- User provides a connection string via the Streamlit UI (PostgreSQL, MySQL, MSSQL, SQLite)
- Connections are validated on submission; credentials are held in session state only — never persisted to disk
- User selects a target table or provides a custom `SELECT` query
- Data is read into a pandas DataFrame via SQLAlchemy (read-only connection at this stage)
- On cleanse execution, the engine writes a new table to the same schema:
  - Default naming: `{source_table}_clean_{timestamp}`
  - User may override the output table name before execution
  - The original source table is **never modified**
- Supported drivers: `psycopg2` (PostgreSQL), `pymysql` (MySQL), `pyodbc` (MSSQL), `sqlite3`

### Shared Behavior (Both Modes)

- Maximum supported dataset size: `DQ_MAX_RECORDS_PER_RUN` (default 50,000 rows). Larger datasets should be chunked upstream or use DB mode with a scoped query.
- Column type inference is performed at ingest and stored in dataset metadata — used by downstream rule checks
- A `dataset_id` (UUID) is assigned at ingest and ties together all issues, approvals, audit log entries, and output artifacts for a given run

---

## Agent Capabilities

### 1. Data Quality Checks

The agent evaluates incoming data against the following rule categories:

| Category | Examples |
|---|---|
| **Completeness** | Null/blank required fields (vendor name, invoice #, amount) |
| **Uniqueness / Deduplication** | Same vendor under two spellings; duplicate invoice numbers |
| **Format Validity** | Dates not in ISO 8601; amounts with currency symbols mixed in |
| **Referential Integrity** | Cost center code not in approved chart of accounts |
| **Consistency** | Same entity with conflicting attributes across records |
| **Outliers / Anomalies** | Invoice amounts > 3σ from vendor's historical average |
| **Domain Rules** | Negative quantities on purchase orders; future-dated invoices |

### 2. Deduplication & Entity Resolution

The primary use case is fuzzy entity matching. The agent:
- Tokenizes and normalizes candidate strings (lowercase, strip punctuation, abbreviation expansion)
- Computes similarity scores (Levenshtein, token sort ratio, phonetic matching)
- Groups records above a configurable confidence threshold as likely duplicates
- Proposes a canonical form for the surviving record
- Presents the match cluster to the user for approval before merging

**Example:**
```
Flagged cluster — Vendor deduplication (confidence: 94%)
  Record A: "Acme Corp."       → ID: V-1042
  Record B: "ACME Corporation" → ID: V-1891
  Proposed canonical: "Acme Corporation" (merge B into A, retire V-1891)
  [ Approve ] [ Reject ] [ Edit canonical ]
```

### 3. Proposed Cleansing Actions

For each issue found, the agent produces a structured proposal:

```json
{
  "issue_id": "DQ-2024-0041",
  "severity": "HIGH",
  "category": "DEDUPLICATION",
  "affected_records": ["V-1042", "V-1891"],
  "description": "Two vendor records appear to represent the same entity.",
  "confidence": 0.94,
  "proposed_action": {
    "type": "MERGE",
    "canonical_value": "Acme Corporation",
    "retain_id": "V-1042",
    "retire_ids": ["V-1891"],
    "remap_transactions": true
  },
  "status": "PENDING_APPROVAL"
}
```

### 4. Human-in-the-Loop Approval

**No data is modified without explicit approval.**

The Streamlit UI presents:
- A summary dashboard of all flagged issues grouped by category and severity
- Individual review cards for each proposed change
- Approve / Reject / Edit actions per issue
- Bulk approve for low-risk, high-confidence issues (configurable threshold)
- Comment field for audit trail notes

### 5. Audit Logging

Every action — check run, approval, rejection, edit, and cleanse — is written to an immutable audit log:

```
timestamp | user | action | issue_id | before_value | after_value | notes
```

---

## Tool Definitions (Claude Tool Use)

The agent uses the following tools exposed via the FastAPI backend:

```python
tools = [
    {
        "name": "ingest_file",
        "description": "Read a CSV or Excel file into the agent's working dataset. Returns dataset_id, row count, column names, and inferred types.",
        "input_schema": {
            "type": "object",
            "properties": {
                "file_path": {"type": "string"},
                "sheet_name": {"type": "string", "description": "Excel only. Defaults to first sheet."}
            },
            "required": ["file_path"]
        }
    },
    {
        "name": "ingest_db_table",
        "description": "Read a database table or query result into the agent's working dataset via a SQLAlchemy connection. Returns dataset_id, row count, column names, and inferred types. Connection is read-only at this stage.",
        "input_schema": {
            "type": "object",
            "properties": {
                "connection_string": {"type": "string"},
                "table_or_query": {"type": "string", "description": "Table name or full SELECT query."}
            },
            "required": ["connection_string", "table_or_query"]
        }
    },
    {
        "name": "run_quality_checks",
        "description": "Run all configured DQ rules against a dataset. Returns a structured report of issues found.",
        "input_schema": {
            "type": "object",
            "properties": {
                "dataset_id": {"type": "string"},
                "rule_categories": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Subset of rule categories to run. Omit to run all."
                }
            },
            "required": ["dataset_id"]
        }
    },
    {
        "name": "get_issue_details",
        "description": "Retrieve full details for a specific DQ issue including affected records and proposed fix.",
        "input_schema": {
            "type": "object",
            "properties": {
                "issue_id": {"type": "string"}
            },
            "required": ["issue_id"]
        }
    },
    {
        "name": "propose_cleanse",
        "description": "Generate a cleansing proposal for a given issue. Does NOT execute — only stages for approval.",
        "input_schema": {
            "type": "object",
            "properties": {
                "issue_id": {"type": "string"},
                "proposed_action": {"type": "object"}
            },
            "required": ["issue_id", "proposed_action"]
        }
    },
    {
        "name": "execute_approved_cleanse",
        "description": "Execute a cleansing action that has been explicitly approved by an authorized user. Writes audit log entry.",
        "input_schema": {
            "type": "object",
            "properties": {
                "issue_id": {"type": "string"},
                "approved_by": {"type": "string"},
                "approval_timestamp": {"type": "string"}
            },
            "required": ["issue_id", "approved_by", "approval_timestamp"]
        }
    },
    {
        "name": "export_clean_file",
        "description": "After all approved cleanses are executed, write the clean dataset to a new output file (CSV or Excel). Returns the output file path for download. Only valid for Mode A (file upload) sessions.",
        "input_schema": {
            "type": "object",
            "properties": {
                "dataset_id": {"type": "string"},
                "output_format": {"type": "string", "enum": ["csv", "xlsx"], "description": "Defaults to same format as input."},
                "output_filename": {"type": "string", "description": "Optional override. Defaults to {original}_clean_{timestamp}."}
            },
            "required": ["dataset_id"]
        }
    },
    {
        "name": "export_clean_table",
        "description": "After all approved cleanses are executed, write the clean dataset to a new table in the source database. Returns the output table name. Only valid for Mode B (DB connection) sessions.",
        "input_schema": {
            "type": "object",
            "properties": {
                "dataset_id": {"type": "string"},
                "output_table_name": {"type": "string", "description": "Optional override. Defaults to {source_table}_clean_{timestamp}."},
                "if_exists": {"type": "string", "enum": ["fail", "replace"], "description": "Behavior if table already exists. Defaults to fail."}
            },
            "required": ["dataset_id"]
        }
    },
    {
        "name": "generate_dq_scorecard",
        "description": "Produce a DQ scorecard summarizing pass/fail rates by category for a dataset.",
        "input_schema": {
            "type": "object",
            "properties": {
                "dataset_id": {"type": "string"},
                "include_trend": {"type": "boolean", "default": false}
            },
            "required": ["dataset_id"]
        }
    }
]
```

---

## System Prompt

```
You are a Data Quality Agent for an FP&A organization. Your job is to analyze 
incoming datasets, identify quality issues, and propose precise, minimal 
cleansing actions to correct them.

You have access to tools that allow you to run checks, retrieve issue details, 
stage cleansing proposals, and execute approved changes. You MUST NOT call 
execute_approved_cleanse unless the issue status is APPROVED and an 
approved_by value is present. Never modify data speculatively.

When evaluating entity matching issues (e.g., duplicate vendor names), 
reason through string similarity, context clues (address, tax ID, contact), 
and historical transaction patterns before proposing a canonical form.

Always return findings in structured JSON. Be conservative: when confidence 
is below 0.80, flag for human review rather than proposing an automatic fix. 
Explain your reasoning for each issue in plain language suitable for a 
non-technical business analyst.
```

---

## Project Structure

```
dq-agent/
├── CLAUDE.md                  # This file
├── README.md
├── .env.example
├── requirements.txt
│
├── backend/
│   ├── main.py                # FastAPI app, routes
│   ├── agent.py               # Claude API calls, tool orchestration
│   ├── tools/
│   │   ├── ingest.py          # File upload + DB connection ingestion
│   │   ├── quality_checks.py  # Rule engine implementations
│   │   ├── entity_resolution.py  # Fuzzy matching logic
│   │   ├── cleanse.py         # Approved-only execution layer
│   │   ├── export.py          # Clean file writer + DB table writer
│   │   └── scorecard.py       # DQ scorecard generation
│   ├── models/
│   │   ├── issue.py           # Issue data model
│   │   └── dataset.py         # Dataset metadata model (source_type, output_target)
│   ├── db/
│   │   ├── database.py        # SQLAlchemy setup (agent internals)
│   │   ├── connections.py     # Session-scoped source DB connection manager
│   │   └── migrations/
│   └── auth/
│       └── rbac.py            # Role-based access control
│
├── frontend/
│   ├── app.py                 # Streamlit entry point
│   ├── pages/
│   │   ├── 01_ingest.py       # Mode selector: file upload OR DB connection
│   │   ├── 02_review.py       # Issue review & approval UI
│   │   ├── 03_export.py       # Download clean file or confirm DB table write
│   │   ├── 04_scorecard.py    # DQ scorecard dashboard
│   │   └── 05_audit_log.py    # Audit trail viewer
│   └── components/
│       └── issue_card.py      # Reusable issue review component
│
└── tests/
    ├── test_ingest.py
    ├── test_quality_checks.py
    ├── test_entity_resolution.py
    ├── test_export.py
    └── fixtures/
        ├── sample_vendor_data.csv
        └── sample_vendor_data.xlsx
```

---

## Environment Variables

```bash
# Anthropic
ANTHROPIC_API_KEY=sk-ant-...

# Agent internals DB (issue registry, audit log)
DATABASE_URL=postgresql://user:pass@host:5432/dq_agent
# or for local dev:
# DATABASE_URL=sqlite:///./dq_agent.db

# Auth
SECRET_KEY=your-secret-key
ACCESS_TOKEN_EXPIRE_MINUTES=60

# Agent config
DQ_CONFIDENCE_THRESHOLD=0.80       # Below this → flag only, no auto-proposal
DQ_BULK_APPROVE_THRESHOLD=0.95     # Above this → eligible for bulk approve
DQ_MAX_RECORDS_PER_RUN=50000

# File output
OUTPUT_DIR=/tmp/dq_outputs          # Where clean files are staged for download

# Source DB connection (Mode B)
# Not stored in env — provided by user at runtime via UI, held in session state only
# Supported drivers (install as needed): psycopg2, pymysql, pyodbc, sqlite3

# Deployment
RAILWAY_ENVIRONMENT=production     # or development
BACKEND_URL=https://your-app.railway.app
```

---

## Role-Based Access Control

| Role | Permissions |
|---|---|
| **Viewer** | View DQ reports and scorecards; no approval rights |
| **Analyst** | View + approve/reject individual low-severity issues |
| **Senior Analyst** | Analyst + bulk approve + approve HIGH severity issues |
| **Admin** | All permissions + configure rules + manage users |

---

## Key Design Decisions

1. **Proposal-first, never execute-first.** Claude stages all changes as proposals with status `PENDING_APPROVAL`. The `execute_approved_cleanse` tool checks for approval status server-side — it is not solely trust-based on the agent's judgment.

2. **Source data is always read-only.** The original uploaded file and the source database table are never modified. All cleansed output goes to a new file or a new table. This makes every run fully reversible.

3. **Output is explicitly named and scoped per run.** Each clean output (file or table) includes a timestamp in its name and is tied to a `dataset_id`, preventing accidental overwrites across runs.

4. **DB credentials are session-scoped only.** Connection strings provided in Mode B are held in Streamlit session state and are never written to disk, logged, or persisted beyond the active session.

5. **Immutable audit log.** All changes — including rejections and edits — are append-only. This supports SOX and federal audit requirements.

6. **Confidence scoring is explicit.** Every issue carries a `confidence` float (0–1). The UI surfaces this prominently so reviewers understand the certainty behind each proposal.

7. **Conservative defaults.** The agent is prompted to prefer flagging over proposing when uncertain. It is easier for a human to approve a correct proposal than to undo an incorrect one.

8. **Entity resolution is human-confirmed.** Merges and deduplication are always individual review items — never bulk-approved — given the downstream impact on transaction history.

9. **Single-user, single-session scope (current).** Concurrency controls are not implemented at this time. The agent is designed for one active session per dataset run. Multi-user support is a planned future enhancement.

---

## Getting Started

```bash
# Clone and install
git clone https://github.com/your-org/dq-agent.git
cd dq-agent
pip install -r requirements.txt

# Configure environment
cp .env.example .env
# Edit .env with your values

# Run database migrations
alembic upgrade head

# Start backend
uvicorn backend.main:app --reload --port 8000

# Start frontend (separate terminal)
streamlit run frontend/app.py
```

---

## Future Enhancements

- [ ] Multi-user concurrency with optimistic locking on the issue registry
- [ ] Scheduled / triggered runs against live DB connections (not just on-demand)
- [ ] ML-based anomaly detection for financial outliers
- [ ] Integration with vendor master / ERP reference data for referential integrity checks
- [ ] Slack / Teams notifications when high-severity issues are detected
- [ ] DQ trend reporting across pipeline runs over time
- [ ] Export approved changes as a SQL migration script for DBA review before DB write
- [ ] Support for additional file formats (JSON, Parquet, pipe-delimited TXT)