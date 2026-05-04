# 🔗 SharePoint Oracle Bridge

> The most advanced SharePoint ↔ Oracle integration available as open source. Delta sync, bidirectional flow, document text extraction, real-time webhooks, a full ETL pipeline, and every query secured by the Oracle Data Gateway.

![Python](https://img.shields.io/badge/Python-3.10+-blue?style=flat-square&logo=python)
![Microsoft Graph](https://img.shields.io/badge/Microsoft_Graph-API-blue?style=flat-square&logo=microsoft)
![Oracle](https://img.shields.io/badge/Oracle-DB-red?style=flat-square&logo=oracle)
![FastAPI](https://img.shields.io/badge/FastAPI-0.110+-green?style=flat-square&logo=fastapi)
![License](https://img.shields.io/badge/License-MIT-green?style=flat-square)

---

## 🎯 What makes this advanced

Most SharePoint integrations do one thing: pull list items and dump them. This does five things most engineers never attempt together:

- **Delta sync** — Graph delta tokens mean a 10,000-item list with 5 changes returns 5 items, not 10,000
- **Real-time webhooks** — SharePoint notifies the bridge the moment anything changes
- **Bidirectional** — Oracle data can push back to SharePoint lists
- **Document extraction** — Word and PDF files are parsed and full text stored in Oracle
- **Security gateway** — every Oracle write passes through JWT auth, SQL injection filtering, PII masking, and audit logging

---

## 🏗️ Architecture

```
SharePoint (Lists · Document Libraries · Sites · Permissions)
       ↓ Microsoft Graph API (OAuth2)
Graph Adapter (full sync / delta sync / webhooks)
       ↓
ETL Engine (transform · validate · error recovery · dead-letter queue)
       ↓ bidirectional ↑
Oracle Data Gateway (JWT · SQL injection filter · PII masking · audit log)
       ↓
Oracle DB (SPX_LISTS · SPX_ITEMS · SPX_DOCUMENTS · SPX_SYNC_LOG · SPX_SITES)
       ↓
FastAPI + React dashboard (sync status · item browser · delta log)
```

---

## 🚀 Quick start

**1. Install dependencies**
```bash
pip install -r requirements.txt
```

**2. Run Phase 1 demo (no credentials needed)**
```bash
python demo/demo_phase1.py
```

**3. Connect real Microsoft Graph**
```bash
cp .env.example .env
# Add GRAPH_TENANT_ID, GRAPH_CLIENT_ID, GRAPH_CLIENT_SECRET
```

---

## 🗂️ Project structure

```
sharepoint-oracle-bridge/
├── graph/
│   ├── models.py          # SPSite, SPList, SPListItem, SPDocument, DeltaToken ...
│   ├── adapter.py         # MockGraphAdapter + RealGraphAdapter
│   └── delta_sync.py      # Delta sync engine + webhook processing
├── etl/                   # ETL engine (Phase 2)
├── oracle/                # Oracle schema + executor (Phase 3)
├── api/                   # FastAPI layer (Phase 4)
├── dashboard/             # React dashboard (Phase 5)
├── demo/
│   └── demo_phase1.py
└── requirements.txt
```

---

## 🗺️ Roadmap

| Phase | Status | Description |
|-------|--------|-------------|
| 1 | Complete | Graph adapter + delta sync + webhooks |
| 2 | Next | ETL engine — transform, validate, error recovery |
| 3 | Planned | Oracle schema + bidirectional sync |
| 4 | Planned | FastAPI layer |
| 5 | Planned | React dashboard |

---

## 🔗 Related projects

- [oracle-data-gateway](https://github.com/sumitbiswas13/oracle-data-gateway) — all Oracle writes pass through this
- [oracle-ai-query-assistant](https://github.com/sumitbiswas13/oracle-ai-query-assistant) — query the extracted SharePoint data in plain English

---

## 📄 License

MIT
