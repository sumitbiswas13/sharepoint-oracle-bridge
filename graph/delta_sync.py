"""
Delta Sync Engine
Orchestrates full and incremental SharePoint sync using Graph delta tokens.
Tracks which items changed since last sync — massive bandwidth saving.
"""

import time
from datetime import datetime
from typing import Optional
from graph.adapter import GraphAdapterBase
from graph.models import (
    SPSite, SPList, SPListItem, SPDocument,
    DeltaToken, SyncResult, WebhookSubscription, WebhookNotification
)


class DeltaSyncEngine:
    """
    Manages incremental sync between SharePoint and the ETL pipeline.

    First sync: fetches everything, stores a delta token per list.
    Subsequent syncs: sends the token, Graph returns ONLY changed items.
    This means a 10,000-item list with 5 changes returns 5 items, not 10,000.
    """

    def __init__(self, adapter: GraphAdapterBase):
        self._adapter     = adapter
        self._tokens:     dict[str, DeltaToken] = {}    # list_id → token
        self._sync_log:   list[SyncResult]       = []
        self._subscriptions: list[WebhookSubscription] = []
        self._pending_webhooks: list[WebhookNotification] = []

    # ── Full sync ─────────────────────────────────────────────

    def full_sync(self, site_id: str) -> list[SyncResult]:
        """Fetch everything from a site — all lists, items, documents, permissions."""
        results = []
        print(f"  Starting full sync for site: {site_id}")

        lists = self._adapter.get_lists(site_id)
        print(f"  Found {len(lists)} lists")

        for sp_list in lists:
            result = self._sync_list(site_id, sp_list, delta_token=None, operation="full_sync")
            results.append(result)
            self._sync_log.append(result)

        return results

    # ── Delta sync ────────────────────────────────────────────

    def delta_sync(self, site_id: str) -> list[SyncResult]:
        """
        Incremental sync — only fetch items changed since last sync.
        Uses stored delta tokens per list.
        """
        results = []
        lists   = self._adapter.get_lists(site_id)

        for sp_list in lists:
            stored = self._tokens.get(sp_list.list_id)
            if not stored:
                # No token yet — do a full sync for this list
                result = self._sync_list(site_id, sp_list, delta_token=None, operation="delta_sync_initial")
            else:
                result = self._sync_list(site_id, sp_list, delta_token=stored.token, operation="delta_sync")

            results.append(result)
            self._sync_log.append(result)

        return results

    # ── Webhook-triggered sync ────────────────────────────────

    def process_webhook(self, notification: WebhookNotification) -> list[SyncResult]:
        """
        Called when SharePoint fires a webhook notification.
        Extracts site/list from the notification and does a targeted delta sync.
        """
        self._pending_webhooks.append(notification)
        results = []

        # Parse resource path — /sites/{siteId}/lists/{listId}/items
        parts   = notification.resource.strip("/").split("/")
        site_id = parts[1] if len(parts) > 1 else None
        list_id = parts[3] if len(parts) > 3 else None

        if not site_id or not list_id:
            return results

        # Find the list
        lists   = self._adapter.get_lists(site_id)
        sp_list = next((l for l in lists if l.list_id == list_id), None)
        if not sp_list:
            return results

        stored = self._tokens.get(list_id)
        result = self._sync_list(
            site_id, sp_list,
            delta_token=stored.token if stored else None,
            operation="webhook_sync",
        )
        results.append(result)
        self._sync_log.append(result)
        return results

    # ── Webhook subscriptions ─────────────────────────────────

    def register_webhooks(self, site_id: str, notification_url: str) -> list[WebhookSubscription]:
        """Register webhook subscriptions for all lists in a site."""
        lists = self._adapter.get_lists(site_id)
        subs  = []
        for sp_list in lists:
            try:
                sub = self._adapter.subscribe_webhook(site_id, sp_list.list_id, notification_url)
                self._subscriptions.append(sub)
                subs.append(sub)
                print(f"  Webhook registered: {sp_list.display_name} → {sub.subscription_id[:8]}...")
            except Exception as e:
                print(f"  Webhook failed for {sp_list.display_name}: {e}")
        return subs

    def refresh_expiring_webhooks(self) -> int:
        """Refresh subscriptions expiring within 3 days. Call daily."""
        from datetime import timedelta
        refreshed = 0
        cutoff    = datetime.utcnow() + timedelta(days=3)
        for sub in self._subscriptions:
            if sub.expiry < cutoff:
                if self._adapter.refresh_webhook(sub.subscription_id):
                    refreshed += 1
        return refreshed

    # ── State ─────────────────────────────────────────────────

    def get_token(self, list_id: str) -> Optional[DeltaToken]:
        return self._tokens.get(list_id)

    def get_all_tokens(self) -> list[DeltaToken]:
        return list(self._tokens.values())

    def get_sync_log(self, limit: int = 20) -> list[SyncResult]:
        return list(reversed(self._sync_log[-limit:]))

    def get_subscriptions(self) -> list[WebhookSubscription]:
        return list(self._subscriptions)

    def get_pending_webhooks(self) -> list[WebhookNotification]:
        return list(self._pending_webhooks)

    def get_stats(self) -> dict:
        total_created = sum(r.items_created for r in self._sync_log)
        total_updated = sum(r.items_updated for r in self._sync_log)
        total_failed  = sum(r.items_failed  for r in self._sync_log)
        return {
            "sync_runs":       len(self._sync_log),
            "active_tokens":   len(self._tokens),
            "subscriptions":   len(self._subscriptions),
            "total_created":   total_created,
            "total_updated":   total_updated,
            "total_failed":    total_failed,
            "pending_webhooks":len(self._pending_webhooks),
        }

    # ── Internal ──────────────────────────────────────────────

    def _sync_list(
        self,
        site_id:     str,
        sp_list:     SPList,
        delta_token: Optional[str],
        operation:   str,
    ) -> SyncResult:
        t0     = time.time()
        result = SyncResult(
            operation=operation,
            resource_id=sp_list.list_id,
            resource_type="list",
        )

        try:
            items, new_token = self._adapter.get_list_items(
                site_id, sp_list.list_id, delta_token=delta_token
            )
            result.items_found = len(items)

            for item in items:
                if item.deleted:
                    result.items_deleted += 1
                elif delta_token:
                    result.items_updated += 1
                else:
                    result.items_created += 1

            # Store/update delta token
            existing = self._tokens.get(sp_list.list_id)
            self._tokens[sp_list.list_id] = DeltaToken(
                resource_id=sp_list.list_id,
                resource_type="list",
                token=new_token,
                items_synced=(existing.items_synced if existing else 0) + len(items),
            )
            result.delta_token = new_token[:12] + "..." if new_token else None

            # Documents if library
            if sp_list.list_type == "documentLibrary":
                docs = self._adapter.get_documents(site_id, sp_list.list_id)
                result.items_found += len(docs)
                result.items_created += len(docs) if not delta_token else 0

        except Exception as e:
            result.items_failed += 1
            result.errors.append(str(e))

        result.duration_ms = round((time.time() - t0) * 1000, 2)
        return result
