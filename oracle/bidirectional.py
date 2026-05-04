"""
Bidirectional Sync Engine
Orchestrates both directions:
  SP_TO_ORACLE: SharePoint changes → ETL pipeline → Oracle tables
  ORACLE_TO_SP: Oracle PUSH_PENDING items → Microsoft Graph API → SharePoint
"""

import time
from datetime import datetime
from graph.adapter import GraphAdapterBase
from graph.delta_sync import DeltaSyncEngine
from graph.models import SPListItem, SyncResult
from oracle.executor import MockOracleExecutor
from etl.pipeline import ETLPipeline


class BidirectionalSyncEngine:
    """
    The core orchestrator. Runs both sync directions and keeps them consistent.

    SP → Oracle:  delta sync → ETL pipeline → upsert/delete in Oracle
    Oracle → SP:  query PUSH_PENDING=1 → Graph API create/update → clear flag
    """

    def __init__(
        self,
        graph_adapter: GraphAdapterBase,
        oracle:        MockOracleExecutor,
    ):
        self._graph   = graph_adapter
        self._oracle  = oracle
        self._delta   = DeltaSyncEngine(graph_adapter)
        self._pipeline = ETLPipeline(load_fn=self._load_to_oracle)
        self._run_log: list[dict] = []

    # ── SP → Oracle ───────────────────────────────────────────

    def sync_sp_to_oracle(self, site_id: str, full: bool = False) -> list[SyncResult]:
        """
        Pull changes from SharePoint and load into Oracle.
        full=True: fetch everything. full=False: delta only.
        """
        t0 = time.time()
        print(f"  [{datetime.utcnow().strftime('%H:%M:%S')}] SP→Oracle {'full' if full else 'delta'} sync: {site_id}")

        # Sync site metadata
        sites = self._graph.get_sites()
        for site in sites:
            if site.site_id == site_id:
                self._oracle.upsert_site(site)

        # Sync lists
        lists = self._graph.get_lists(site_id)
        for sp_list in lists:
            stored_token = self._oracle.get_delta_token(sp_list.list_id)
            token = None if full else stored_token

            items, new_token = self._graph.get_list_items(site_id, sp_list.list_id, delta_token=token)
            self._oracle.upsert_list(sp_list, delta_token=new_token)
            self._oracle.update_delta_token(sp_list.list_id, new_token)

            # Upsert / soft-delete items
            created = updated = deleted = 0
            for item in items:
                if item.deleted:
                    self._oracle.soft_delete_item(item.item_id)
                    deleted += 1
                else:
                    existing = next((r for r in self._oracle.get_items(sp_list.list_id)
                                     if r["ITEM_ID"] == item.item_id), None)
                    self._oracle.upsert_item(item)
                    if existing:
                        updated += 1
                    else:
                        created += 1

            # Documents
            if sp_list.list_type == "documentLibrary":
                docs = self._graph.get_documents(site_id, sp_list.list_id)
                for doc in docs:
                    self._oracle.upsert_document(doc)

            result = SyncResult(
                operation="full_sync" if full else "delta_sync",
                resource_id=sp_list.list_id,
                resource_type="list",
                items_found=len(items),
                items_created=created,
                items_updated=updated,
                items_deleted=deleted,
                delta_token=new_token[:12] + "..." if new_token else None,
                duration_ms=round((time.time() - t0) * 1000, 2),
            )
            self._oracle.log_sync(result, direction="SP_TO_ORACLE")

        elapsed = round((time.time() - t0) * 1000, 2)
        self._run_log.append({
            "direction": "SP_TO_ORACLE",
            "site_id":   site_id,
            "lists":     len(lists),
            "duration_ms": elapsed,
            "at":        datetime.utcnow().isoformat(),
        })

        return [SyncResult(
            operation="full_sync" if full else "delta_sync",
            resource_id=site_id,
            resource_type="site",
            items_found=sum(len(self._oracle.get_items(l.list_id)) for l in lists),
            duration_ms=elapsed,
        )]

    # ── Oracle → SP ───────────────────────────────────────────

    def sync_oracle_to_sp(self) -> SyncResult:
        """
        Push Oracle-originated items (PUSH_PENDING=1) back to SharePoint.
        Called after any Oracle-side update that should reflect in SharePoint.
        """
        t0 = time.time()
        pending = self._oracle.get_items(push_pending=True)
        print(f"  [{datetime.utcnow().strftime('%H:%M:%S')}] Oracle→SP push: {len(pending)} items pending")

        created = updated = failed = 0

        for row in pending:
            list_id = row.get("LIST_ID","")
            site_id = row.get("SITE_ID","")
            item_id = row.get("ITEM_ID","")

            # Build SharePoint fields from Oracle row
            sp_fields = {
                "Title":      row.get("TITLE",""),
                "Status":     row.get("STATUS",""),
                "Priority":   row.get("PRIORITY",""),
                "AssignedTo": row.get("ASSIGNED_TO",""),
            }
            sp_fields = {k: v for k, v in sp_fields.items() if v}

            try:
                # Does it already exist in SharePoint?
                existing_etag = row.get("SP_ETAG","")
                if existing_etag and not existing_etag.startswith("item-oracle"):
                    # Update existing
                    ok = self._graph.update_list_item(site_id, list_id, existing_etag, sp_fields)
                    if ok:
                        self._oracle.clear_push_pending(item_id, existing_etag)
                        updated += 1
                else:
                    # Create new
                    new_item = self._graph.create_list_item(site_id, list_id, sp_fields)
                    self._oracle.clear_push_pending(item_id, new_item.item_id)
                    created += 1
            except Exception as e:
                failed += 1

        result = SyncResult(
            operation="oracle_to_sp_push",
            resource_id="all",
            resource_type="push_pending",
            items_found=len(pending),
            items_created=created,
            items_updated=updated,
            items_failed=failed,
            duration_ms=round((time.time() - t0) * 1000, 2),
        )
        self._oracle.log_sync(result, direction="ORACLE_TO_SP")
        self._run_log.append({
            "direction":  "ORACLE_TO_SP",
            "pending":    len(pending),
            "created":    created,
            "updated":    updated,
            "failed":     failed,
            "duration_ms":result.duration_ms,
            "at":         datetime.utcnow().isoformat(),
        })
        return result

    # ── Queue Oracle item for push ────────────────────────────

    def queue_for_sharepoint(self, item_id: str, list_id: str, site_id: str, fields: dict):
        """Flag an Oracle item to be pushed to SharePoint on next sync."""
        self._oracle.set_push_pending(item_id, {
            "LIST_ID": list_id, "SITE_ID": site_id, **fields,
        })

    def get_run_log(self) -> list[dict]:
        return list(reversed(self._run_log))

    def get_stats(self) -> dict:
        return {
            "oracle_stats": self._oracle.get_stats(),
            "sync_runs":    len(self._run_log),
            "sp_to_oracle": sum(1 for r in self._run_log if r["direction"] == "SP_TO_ORACLE"),
            "oracle_to_sp": sum(1 for r in self._run_log if r["direction"] == "ORACLE_TO_SP"),
        }

    def _load_to_oracle(self, oracle_row: dict) -> bool:
        """ETL pipeline load function — writes transformed rows to Oracle."""
        item = SPListItem(
            item_id=oracle_row.get("SP_ITEM_ID",""),
            list_id=oracle_row.get("LIST_ID",""),
            site_id=oracle_row.get("SITE_ID",""),
            etag=oracle_row.get("SP_ETAG",""),
            fields={k: v for k, v in oracle_row.items()
                    if k not in ("SP_ITEM_ID","LIST_ID","SITE_ID","SP_ETAG",
                                  "SP_CREATED_AT","SP_MODIFIED_AT","SP_CREATED_BY","SP_MODIFIED_BY")},
        )
        return self._oracle.upsert_item(item)
