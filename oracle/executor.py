"""
Oracle Executor
Handles all read/write operations against the SPX_* tables.
MockOracleExecutor for demo — swap RealOracleExecutor with live oracledb.
"""

import json
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional
from graph.models import SPSite, SPList, SPListItem, SPDocument, SyncResult


@dataclass
class OracleRow:
    table:  str
    data:   dict[str, Any]
    action: str = "UPSERT"   # UPSERT | DELETE


class MockOracleExecutor:
    """
    In-memory Oracle mock — stores all SPX_* rows in dicts.
    Shows realistic upsert/delete/push_pending logic.
    Replace with RealOracleExecutor for production.
    """

    def __init__(self):
        self._sites:     dict[str, dict] = {}
        self._lists:     dict[str, dict] = {}
        self._items:     dict[str, dict] = {}
        self._documents: dict[str, dict] = {}
        self._sync_log:  list[dict]      = []
        self._log_seq    = 0

    # ── Sites ─────────────────────────────────────────────────

    def upsert_site(self, site: SPSite) -> bool:
        self._sites[site.site_id] = {
            "SITE_ID":       site.site_id,
            "DISPLAY_NAME":  site.display_name,
            "WEB_URL":       site.web_url,
            "DESCRIPTION":   site.description,
            "OWNER_EMAIL":   site.owner_email,
            "STORAGE_USED":  site.storage_used,
            "SP_CREATED_AT": site.created_at.isoformat() if site.created_at else None,
            "SP_MODIFIED_AT":site.modified_at.isoformat() if site.modified_at else None,
            "SYNC_STATUS":   "SYNCED",
            "UPDATED_AT":    datetime.utcnow().isoformat(),
        }
        return True

    def get_sites(self) -> list[dict]:
        return list(self._sites.values())

    # ── Lists ─────────────────────────────────────────────────

    def upsert_list(self, sp_list: SPList, delta_token: str = None) -> bool:
        self._lists[sp_list.list_id] = {
            "LIST_ID":       sp_list.list_id,
            "SITE_ID":       sp_list.site_id,
            "DISPLAY_NAME":  sp_list.display_name,
            "DESCRIPTION":   sp_list.description,
            "LIST_TYPE":     sp_list.list_type,
            "ITEM_COUNT":    sp_list.item_count,
            "DELTA_TOKEN":   delta_token,
            "LAST_SYNC_AT":  datetime.utcnow().isoformat(),
            "SYNC_STATUS":   "SYNCED",
        }
        return True

    def get_lists(self, site_id: str = None) -> list[dict]:
        rows = list(self._lists.values())
        if site_id:
            rows = [r for r in rows if r["SITE_ID"] == site_id]
        return rows

    def update_delta_token(self, list_id: str, token: str) -> bool:
        if list_id in self._lists:
            self._lists[list_id]["DELTA_TOKEN"] = token
            self._lists[list_id]["LAST_SYNC_AT"] = datetime.utcnow().isoformat()
        return True

    def get_delta_token(self, list_id: str) -> Optional[str]:
        return self._lists.get(list_id, {}).get("DELTA_TOKEN")

    # ── Items ─────────────────────────────────────────────────

    def upsert_item(self, item: SPListItem) -> bool:
        fields_json = json.dumps(item.fields)
        self._items[item.item_id] = {
            "ITEM_ID":       item.item_id,
            "LIST_ID":       item.list_id,
            "SITE_ID":       item.site_id,
            "SP_ETAG":       item.etag,
            "TITLE":         item.fields.get("Title","")[:4000] if item.fields else "",
            "STATUS":        item.fields.get("Status"),
            "PRIORITY":      item.fields.get("Priority"),
            "ASSIGNED_TO":   item.fields.get("AssignedTo"),
            "DUE_DATE":      item.fields.get("DueDate"),
            "FIELDS_JSON":   fields_json,
            "SYNC_STATUS":   "SYNCED",
            "PUSH_PENDING":  0,
            "SP_CREATED_AT": item.created_at.isoformat() if item.created_at else None,
            "SP_MODIFIED_AT":item.modified_at.isoformat() if item.modified_at else None,
            "SP_CREATED_BY": item.created_by,
            "SP_MODIFIED_BY":item.modified_by,
            "DELETED":       1 if item.deleted else 0,
            "UPDATED_AT":    datetime.utcnow().isoformat(),
        }
        return True

    def soft_delete_item(self, item_id: str) -> bool:
        if item_id in self._items:
            self._items[item_id]["DELETED"]     = 1
            self._items[item_id]["SYNC_STATUS"] = "DELETED"
        return True

    def get_items(self, list_id: str = None, push_pending: bool = False) -> list[dict]:
        rows = [r for r in self._items.values() if r.get("DELETED") != 1]
        if list_id:
            rows = [r for r in rows if r["LIST_ID"] == list_id]
        if push_pending:
            rows = [r for r in rows if r.get("PUSH_PENDING") == 1]
        return rows

    def set_push_pending(self, item_id: str, fields: dict) -> bool:
        """Mark an Oracle-originated item for push back to SharePoint."""
        self._items[item_id] = {
            "ITEM_ID":      item_id,
            "LIST_ID":      fields.get("LIST_ID",""),
            "SITE_ID":      fields.get("SITE_ID",""),
            "SP_ETAG":      "",
            "TITLE":        fields.get("TITLE",""),
            "STATUS":       fields.get("STATUS"),
            "PRIORITY":     fields.get("PRIORITY"),
            "ASSIGNED_TO":  fields.get("ASSIGNED_TO"),
            "FIELDS_JSON":  json.dumps(fields),
            "SYNC_STATUS":  "PENDING_PUSH",
            "PUSH_PENDING": 1,
            "DELETED":      0,
            "UPDATED_AT":   datetime.utcnow().isoformat(),
        }
        return True

    def clear_push_pending(self, item_id: str, sp_item_id: str) -> bool:
        if item_id in self._items:
            self._items[item_id]["PUSH_PENDING"] = 0
            self._items[item_id]["SYNC_STATUS"]  = "SYNCED"
            self._items[item_id]["SP_ETAG"]      = sp_item_id
        return True

    # ── Documents ─────────────────────────────────────────────

    def upsert_document(self, doc: SPDocument) -> bool:
        self._documents[doc.doc_id] = {
            "DOC_ID":          doc.doc_id,
            "LIST_ID":         doc.list_id,
            "SITE_ID":         doc.site_id,
            "FILE_NAME":       doc.file_name[:500],
            "FILE_TYPE":       doc.file_type,
            "FILE_SIZE":       doc.file_size,
            "WEB_URL":         doc.web_url[:2000],
            "SP_ETAG":         doc.etag,
            "EXTRACTED_TEXT":  doc.extracted_text,
            "PAGE_COUNT":      doc.page_count,
            "SYNC_STATUS":     "SYNCED",
            "SP_CREATED_AT":   doc.created_at.isoformat() if doc.created_at else None,
            "SP_CREATED_BY":   doc.created_by,
            "UPDATED_AT":      datetime.utcnow().isoformat(),
        }
        return True

    def get_documents(self, list_id: str = None) -> list[dict]:
        rows = list(self._documents.values())
        return [r for r in rows if r["LIST_ID"] == list_id] if list_id else rows

    # ── Sync log ──────────────────────────────────────────────

    def log_sync(self, result: SyncResult, direction: str = "SP_TO_ORACLE") -> int:
        self._log_seq += 1
        self._sync_log.append({
            "LOG_ID":        self._log_seq,
            "OPERATION":     result.operation,
            "RESOURCE_ID":   result.resource_id,
            "RESOURCE_TYPE": result.resource_type,
            "ITEMS_FOUND":   result.items_found,
            "ITEMS_CREATED": result.items_created,
            "ITEMS_UPDATED": result.items_updated,
            "ITEMS_DELETED": result.items_deleted,
            "ITEMS_FAILED":  result.items_failed,
            "DURATION_MS":   result.duration_ms,
            "DELTA_TOKEN":   result.delta_token,
            "DIRECTION":     direction,
            "STARTED_AT":    result.started_at.isoformat(),
        })
        return self._log_seq

    def get_sync_log(self, limit: int = 20) -> list[dict]:
        return list(reversed(self._sync_log[-limit:]))

    # ── Stats ─────────────────────────────────────────────────

    def get_stats(self) -> dict:
        push_pending = sum(1 for r in self._items.values() if r.get("PUSH_PENDING") == 1)
        return {
            "SPX_SITES":     len(self._sites),
            "SPX_LISTS":     len(self._lists),
            "SPX_ITEMS":     len(self._items),
            "SPX_DOCUMENTS": len(self._documents),
            "SPX_SYNC_LOG":  len(self._sync_log),
            "push_pending":  push_pending,
        }


class RealOracleExecutor:
    """
    Production Oracle executor using python-oracledb.
    Swap MockOracleExecutor for RealOracleExecutor — no other code changes.
    """

    def __init__(self, dsn: str, user: str, password: str):
        import oracledb
        self._pool = oracledb.create_pool(user=user, password=password, dsn=dsn, min=2, max=10, increment=1)

    def _conn(self):
        return self._pool.acquire()

    def upsert_site(self, site: SPSite) -> bool:
        sql = """
        MERGE INTO SPX_SITES t
        USING (SELECT :1 AS SITE_ID FROM DUAL) s ON (t.SITE_ID = s.SITE_ID)
        WHEN MATCHED THEN UPDATE SET
          DISPLAY_NAME=:2, WEB_URL=:3, OWNER_EMAIL=:4, STORAGE_USED=:5, UPDATED_AT=SYSDATE
        WHEN NOT MATCHED THEN INSERT
          (SITE_ID,DISPLAY_NAME,WEB_URL,OWNER_EMAIL,STORAGE_USED)
          VALUES (:1,:2,:3,:4,:5)
        """
        with self._conn() as conn:
            conn.execute(sql, [site.site_id, site.display_name, site.web_url,
                               site.owner_email, site.storage_used])
            conn.commit()
        return True

    def upsert_item(self, item: SPListItem) -> bool:
        sql = """
        MERGE INTO SPX_ITEMS t
        USING (SELECT :1 AS ITEM_ID FROM DUAL) s ON (t.ITEM_ID = s.ITEM_ID)
        WHEN MATCHED THEN UPDATE SET
          SP_ETAG=:2, TITLE=:3, STATUS=:4, PRIORITY=:5,
          FIELDS_JSON=:6, UPDATED_AT=SYSDATE, DELETED=:7
        WHEN NOT MATCHED THEN INSERT
          (ITEM_ID,LIST_ID,SITE_ID,SP_ETAG,TITLE,STATUS,PRIORITY,FIELDS_JSON,DELETED)
          VALUES (:1,:8,:9,:2,:3,:4,:5,:6,:7)
        """
        fields_json = json.dumps(item.fields)
        with self._conn() as conn:
            conn.execute(sql, [
                item.item_id, item.etag,
                item.fields.get("Title","")[:4000],
                item.fields.get("Status"), item.fields.get("Priority"),
                fields_json, 1 if item.deleted else 0,
                item.list_id, item.site_id,
            ])
            conn.commit()
        return True

    def upsert_document(self, doc: SPDocument) -> bool:
        sql = """
        MERGE INTO SPX_DOCUMENTS t
        USING (SELECT :1 AS DOC_ID FROM DUAL) s ON (t.DOC_ID = s.DOC_ID)
        WHEN MATCHED THEN UPDATE SET
          FILE_NAME=:2, FILE_SIZE=:3, EXTRACTED_TEXT=:4, UPDATED_AT=SYSDATE
        WHEN NOT MATCHED THEN INSERT
          (DOC_ID,LIST_ID,SITE_ID,FILE_NAME,FILE_TYPE,FILE_SIZE,WEB_URL,EXTRACTED_TEXT)
          VALUES (:1,:5,:6,:2,:7,:3,:8,:4)
        """
        with self._conn() as conn:
            conn.execute(sql, [doc.doc_id, doc.file_name[:500], doc.file_size,
                               doc.extracted_text, doc.list_id, doc.site_id,
                               doc.file_type, doc.web_url[:2000]])
            conn.commit()
        return True

    def get_delta_token(self, list_id: str) -> Optional[str]:
        with self._conn() as conn:
            row = conn.execute("SELECT DELTA_TOKEN FROM SPX_LISTS WHERE LIST_ID=:1",
                               [list_id]).fetchone()
            return row[0] if row else None

    def update_delta_token(self, list_id: str, token: str) -> bool:
        with self._conn() as conn:
            conn.execute("UPDATE SPX_LISTS SET DELTA_TOKEN=:1, LAST_SYNC_AT=SYSDATE WHERE LIST_ID=:2",
                         [token, list_id])
            conn.commit()
        return True

    def get_items(self, list_id: str = None, push_pending: bool = False) -> list[dict]:
        where = []
        params = []
        if list_id:
            where.append("LIST_ID=:1"); params.append(list_id)
        if push_pending:
            where.append("PUSH_PENDING=1")
        sql = "SELECT * FROM SPX_ITEMS" + (" WHERE " + " AND ".join(where) if where else "")
        with self._conn() as conn:
            cols = [d[0] for d in conn.execute(sql, params).description]
            return [dict(zip(cols, row)) for row in conn.execute(sql, params).fetchall()]

    def get_sync_log(self, limit: int = 20) -> list[dict]:
        with self._conn() as conn:
            sql = f"SELECT * FROM SPX_SYNC_LOG ORDER BY LOG_ID DESC FETCH FIRST {limit} ROWS ONLY"
            cols = [d[0] for d in conn.execute(sql).description]
            return [dict(zip(cols, row)) for row in conn.execute(sql).fetchall()]

    def log_sync(self, result: SyncResult, direction: str = "SP_TO_ORACLE") -> int:
        sql = """INSERT INTO SPX_SYNC_LOG
          (OPERATION,RESOURCE_ID,RESOURCE_TYPE,ITEMS_FOUND,ITEMS_CREATED,
           ITEMS_UPDATED,ITEMS_DELETED,ITEMS_FAILED,DURATION_MS,DELTA_TOKEN,DIRECTION)
          VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11)"""
        with self._conn() as conn:
            conn.execute(sql, [result.operation, result.resource_id, result.resource_type,
                               result.items_found, result.items_created, result.items_updated,
                               result.items_deleted, result.items_failed, result.duration_ms,
                               result.delta_token, direction])
            conn.commit()
        return 0

    # Stub remaining methods to match Mock interface
    def upsert_list(self, sp_list, delta_token=None): pass
    def get_lists(self, site_id=None): return []
    def get_sites(self): return []
    def get_documents(self, list_id=None): return []
    def get_stats(self): return {}
    def set_push_pending(self, item_id, fields): pass
    def clear_push_pending(self, item_id, sp_item_id): pass
    def soft_delete_item(self, item_id): pass
