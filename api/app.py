"""
SharePoint Oracle Bridge — FastAPI App
Run with: python -m uvicorn api.app:app --reload
Docs at:  http://localhost:8000/docs
"""

from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, BackgroundTasks, Request, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
import os

from graph.adapter import MockGraphAdapter, RealGraphAdapter
from oracle.executor import MockOracleExecutor
from oracle.bidirectional import BidirectionalSyncEngine
from graph.models import WebhookNotification


# ── Request models ────────────────────────────────────────────

class SyncRequest(BaseModel):
    site_id: str
    full:    bool = False

class PushRequest(BaseModel):
    item_id:  str
    list_id:  str
    site_id:  str
    fields:   dict

class WebhookRegisterRequest(BaseModel):
    site_id:          str
    notification_url: str

class WebhookPayload(BaseModel):
    value: list[dict]


# ── Lifespan ──────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("🔗 SharePoint Oracle Bridge starting...")

    adapter = MockGraphAdapter()
    oracle  = MockOracleExecutor()
    engine  = BidirectionalSyncEngine(adapter, oracle)

    # Seed with a full sync on startup
    engine.sync_sp_to_oracle("site-001", full=True)
    engine.sync_sp_to_oracle("site-002", full=True)

    app.state.engine  = engine
    app.state.adapter = adapter
    app.state.oracle  = oracle

    print(f"  Sites synced: 2")
    print(f"  Oracle stats: {oracle.get_stats()}")
    print("✅ Ready — docs at http://localhost:8000/docs")
    yield
    print("👋 Shutting down...")


# ── App ───────────────────────────────────────────────────────

app = FastAPI(
    title="SharePoint Oracle Bridge",
    description="""
## 🔗 SharePoint Oracle Bridge

Advanced bidirectional sync between SharePoint and Oracle DB.

### Key capabilities
- **Delta sync** — only fetch changed items using Graph delta tokens
- **Bidirectional** — Oracle data pushes back to SharePoint
- **Webhooks** — real-time triggers when SharePoint changes
- **Document extraction** — Word/PDF text stored in Oracle CLOBs
- **Full ETL pipeline** — transform, validate, error recovery, DLQ

### Sync flow
```
POST /sync/sp-to-oracle  →  fetch SharePoint changes  →  load Oracle
POST /sync/oracle-to-sp  →  find PUSH_PENDING items   →  push to SharePoint
POST /sync/webhook        →  incoming SP notification  →  targeted delta sync
```
    """,
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


# ── Health ────────────────────────────────────────────────────

@app.get("/", tags=["Health"])
def root():
    return {
        "system":  "SharePoint Oracle Bridge",
        "status":  "active",
        "docs":    "/docs",
        "endpoints": {
            "sync_sp_to_oracle": "POST /sync/sp-to-oracle",
            "sync_oracle_to_sp": "POST /sync/oracle-to-sp",
            "webhook_receive":   "POST /sync/webhook",
            "sites":             "GET /sites",
            "lists":             "GET /sites/{site_id}/lists",
            "items":             "GET /lists/{list_id}/items",
            "documents":         "GET /lists/{list_id}/documents",
            "sync_log":          "GET /sync/log",
            "dlq":               "GET /sync/dlq",
        },
    }


@app.get("/health", tags=["Health"])
def health():
    stats = app.state.oracle.get_stats()
    return {"status": "healthy", "oracle": stats}


# ── Sync endpoints ────────────────────────────────────────────

@app.post("/sync/sp-to-oracle", tags=["Sync"])
async def sync_sp_to_oracle(body: SyncRequest, background_tasks: BackgroundTasks):
    """
    Pull SharePoint changes into Oracle.
    full=false uses delta tokens — only changed items fetched.
    full=true fetches everything from scratch.
    """
    def run_sync():
        app.state.engine.sync_sp_to_oracle(body.site_id, full=body.full)

    background_tasks.add_task(run_sync)
    return {
        "status":  "accepted",
        "site_id": body.site_id,
        "mode":    "full" if body.full else "delta",
        "message": "Sync running in background — check /sync/log for results",
    }


@app.post("/sync/oracle-to-sp", tags=["Sync"])
def sync_oracle_to_sp():
    """
    Push Oracle-originated items (PUSH_PENDING=1) back to SharePoint.
    Clears PUSH_PENDING flag on success.
    """
    result = app.state.engine.sync_oracle_to_sp()
    return {
        "items_found":   result.items_found,
        "created_in_sp": result.items_created,
        "updated_in_sp": result.items_updated,
        "failed":        result.items_failed,
        "duration_ms":   result.duration_ms,
    }


@app.post("/sync/webhook", tags=["Sync"])
async def receive_webhook(request: Request, background_tasks: BackgroundTasks):
    """
    SharePoint webhook notification receiver.
    Graph API sends a validationToken on first call — must echo it back.
    On real notifications, triggers a targeted delta sync.
    """
    # Validation handshake
    validation_token = request.query_params.get("validationToken")
    if validation_token:
        from fastapi.responses import PlainTextResponse
        return PlainTextResponse(validation_token, media_type="text/plain")

    try:
        body = await request.json()
        notifications = body.get("value", [])
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid webhook payload")

    def process():
        for n in notifications:
            notif = WebhookNotification(
                subscription_id=n.get("subscriptionId",""),
                client_state=n.get("clientState",""),
                expiry=n.get("subscriptionExpirationDateTime",""),
                resource=n.get("resource",""),
                site_url=n.get("siteUrl",""),
                tenant_id=n.get("tenantId",""),
            )
            app.state.engine._delta.process_webhook(notif)

    background_tasks.add_task(process)
    return {"status": "accepted", "notifications": len(notifications)}


@app.post("/sync/webhooks/register", tags=["Sync"])
def register_webhooks(body: WebhookRegisterRequest):
    """Register SharePoint webhook subscriptions for all lists in a site."""
    subs = app.state.engine._delta.register_webhooks(
        body.site_id, body.notification_url
    )
    return {
        "registered": len(subs),
        "subscriptions": [
            {
                "subscription_id": s.subscription_id[:16] + "...",
                "resource":        s.resource,
                "expires":         s.expiry.isoformat(),
            }
            for s in subs
        ],
    }


@app.get("/sync/log", tags=["Sync"])
def get_sync_log(limit: int = Query(20, ge=1, le=100)):
    """Audit log of all sync operations."""
    return {
        "entries": app.state.oracle.get_sync_log(limit=limit),
        "stats":   app.state.engine.get_stats(),
    }


@app.get("/sync/dlq", tags=["Sync"])
def get_dlq():
    """Dead-letter queue — items that failed all retries."""
    dlq = app.state.engine._pipeline.get_dlq_summary()
    return {
        "pending": len(dlq),
        "items":   dlq,
    }


# ── Sites ─────────────────────────────────────────────────────

@app.get("/sites", tags=["Sites"])
def list_sites():
    """List all synced SharePoint sites."""
    return {"sites": app.state.oracle.get_sites()}


@app.get("/sites/{site_id}/lists", tags=["Sites"])
def list_site_lists(site_id: str):
    """List all lists for a site."""
    lists = app.state.oracle.get_lists(site_id=site_id)
    if not lists:
        raise HTTPException(status_code=404, detail={"error": "SITE_NOT_FOUND", "site_id": site_id})
    return {"site_id": site_id, "lists": lists}


# ── Items ─────────────────────────────────────────────────────

@app.get("/lists/{list_id}/items", tags=["Items"])
def get_items(
    list_id:      str,
    status:       Optional[str] = None,
    push_pending: bool = False,
    limit:        int  = Query(50, ge=1, le=500),
):
    """Get items from a list. Filter by status or push_pending flag."""
    items = app.state.oracle.get_items(list_id=list_id, push_pending=push_pending)
    if status:
        items = [i for i in items if i.get("STATUS","").lower() == status.lower()]
    return {
        "list_id":   list_id,
        "total":     len(items),
        "items":     items[:limit],
    }


@app.post("/lists/{list_id}/items/push", tags=["Items"])
def queue_item_for_push(list_id: str, body: PushRequest):
    """
    Queue an Oracle item to be pushed to SharePoint on next sync.
    Sets PUSH_PENDING=1 — picked up by POST /sync/oracle-to-sp.
    """
    app.state.engine.queue_for_sharepoint(
        body.item_id, list_id, body.site_id, body.fields
    )
    return {
        "status":   "queued",
        "item_id":  body.item_id,
        "message":  "Item will be pushed to SharePoint on next sync — POST /sync/oracle-to-sp",
    }


# ── Documents ─────────────────────────────────────────────────

@app.get("/lists/{list_id}/documents", tags=["Documents"])
def get_documents(list_id: str, file_type: Optional[str] = None):
    """Get documents from a library with their extracted text."""
    docs = app.state.oracle.get_documents(list_id=list_id)
    if file_type:
        docs = [d for d in docs if d.get("FILE_TYPE","").lower() == file_type.lower()]
    return {
        "list_id":   list_id,
        "total":     len(docs),
        "documents": docs,
    }


@app.get("/documents/search", tags=["Documents"])
def search_documents(q: str, limit: int = Query(10, ge=1, le=50)):
    """Full-text search across extracted document text."""
    all_docs = app.state.oracle.get_documents()
    q_lower  = q.lower()
    matches  = [
        d for d in all_docs
        if q_lower in (d.get("EXTRACTED_TEXT") or "").lower()
        or q_lower in (d.get("FILE_NAME") or "").lower()
    ]
    return {
        "query":   q,
        "total":   len(matches),
        "results": [
            {
                "doc_id":    d["DOC_ID"],
                "file_name": d["FILE_NAME"],
                "file_type": d["FILE_TYPE"],
                "snippet":   _snippet(d.get("EXTRACTED_TEXT",""), q),
            }
            for d in matches[:limit]
        ],
    }


def _snippet(text: str, query: str, context: int = 80) -> str:
    idx = text.lower().find(query.lower())
    if idx == -1:
        return text[:160]
    start = max(0, idx - context)
    end   = min(len(text), idx + len(query) + context)
    return ("..." if start > 0 else "") + text[start:end] + ("..." if end < len(text) else "")
