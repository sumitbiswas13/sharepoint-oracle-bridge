"""
SharePoint Oracle Bridge — Phase 1 Demo
Microsoft Graph adapter + delta sync + webhook support.
Run with: python demo/demo_phase1.py
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from graph.adapter import MockGraphAdapter
from graph.delta_sync import DeltaSyncEngine
from graph.models import WebhookNotification

adapter = MockGraphAdapter()
engine  = DeltaSyncEngine(adapter)

print("=" * 65)
print("  SharePoint Oracle Bridge — Phase 1")
print("  Microsoft Graph Adapter + Delta Sync + Webhooks")
print("=" * 65)

# ── Sites discovery ───────────────────────────────────────────
print("\n🌐 SHAREPOINT SITES DISCOVERED\n")
sites = adapter.get_sites()
for s in sites:
    gb = s.storage_used / 1_000_000_000
    print(f"  {s.site_id}  {s.display_name:<28}  {gb:.1f} GB  {s.owner_email}")

# ── Lists discovery ───────────────────────────────────────────
print("\n📋 LISTS PER SITE\n")
for site in sites[:2]:
    lists = adapter.get_lists(site.site_id)
    print(f"  {site.display_name}:")
    for l in lists:
        doc_tag = " [DOC LIBRARY]" if l.list_type == "documentLibrary" else ""
        print(f"    {l.list_id}  {l.display_name:<30} {l.item_count:>4} items{doc_tag}")

# ── Full sync ─────────────────────────────────────────────────
print(f"\n{'─'*65}")
print("  FULL SYNC — site-001 (Acme Corp Intranet)")
print(f"{'─'*65}\n")
results = engine.full_sync("site-001")
for r in results:
    token_preview = r.delta_token[:15] + "..." if r.delta_token else "none"
    print(f"  {r.resource_id:<14}  found={r.items_found:<4} created={r.items_created:<4} "
          f"duration={r.duration_ms:.0f}ms  token={token_preview}")

# ── Delta sync — only changed items ──────────────────────────
print(f"\n{'─'*65}")
print("  DELTA SYNC — same site (only changed items returned)")
print(f"{'─'*65}\n")
delta_results = engine.delta_sync("site-001")
total_full  = sum(r.items_created for r in results)
total_delta = sum(r.items_updated for r in delta_results)
print(f"  Full sync fetched  : {total_full} items")
print(f"  Delta sync fetched : {total_delta} items (only what changed)")
print(f"  Bandwidth saving   : {100 - int(total_delta/max(total_full,1)*100)}%\n")
for r in delta_results:
    print(f"  {r.resource_id:<14}  updated={r.items_updated:<4} duration={r.duration_ms:.0f}ms  op={r.operation}")

# ── Document extraction ───────────────────────────────────────
print(f"\n{'─'*65}")
print("  DOCUMENT EXTRACTION — HR Policies Library")
print(f"{'─'*65}\n")
docs = adapter.get_documents("site-002", "list-006")
for d in docs:
    kb   = d.file_size // 1024
    text = (d.extracted_text or "")[:60] + "..."
    print(f"  [{d.file_type.upper():<4}] {d.file_name:<40} {kb:>4}KB")
    print(f"         Text: {text}")

# ── Permissions ───────────────────────────────────────────────
print(f"\n{'─'*65}")
print("  PERMISSIONS — Acme Corp Intranet")
print(f"{'─'*65}\n")
perms = adapter.get_permissions("site-001")
for p in perms:
    print(f"  {p.role:<10}  {p.principal}")

# ── Webhook simulation ────────────────────────────────────────
print(f"\n{'─'*65}")
print("  WEBHOOK SUBSCRIPTIONS")
print(f"{'─'*65}\n")
subs = engine.register_webhooks("site-001", "https://bridge.acme.com/webhooks/sharepoint")
for sub in subs:
    print(f"  Registered: {sub.subscription_id[:12]}...  expires {sub.expiry.strftime('%Y-%m-%d')}  "
          f"resource: {sub.resource[-30:]}")

print(f"\n  Simulating incoming webhook notification...")
notif = WebhookNotification(
    subscription_id=subs[0].subscription_id,
    client_state=subs[0].client_state,
    expiry=subs[0].expiry.isoformat(),
    resource=subs[0].resource,
    site_url="https://acme.sharepoint.com/sites/intranet",
    tenant_id="acme-tenant-001",
)
webhook_results = engine.process_webhook(notif)
for r in webhook_results:
    print(f"  Webhook sync: {r.resource_id}  updated={r.items_updated}  op={r.operation}")

# ── Bidirectional push ────────────────────────────────────────
print(f"\n{'─'*65}")
print("  BIDIRECTIONAL — Push Oracle data to SharePoint")
print(f"{'─'*65}\n")
new_item = adapter.create_list_item("site-001", "list-001", {
    "Title":      "Q4 Payroll Completed",
    "Status":     "Complete",
    "Priority":   "High",
    "AssignedTo": "oracle-bridge@acme.com",
})
print(f"  Created SharePoint item: {new_item.item_id}")
print(f"  Fields: {new_item.fields}")

updated = adapter.update_list_item("site-001","list-001","item-list-001-0001",{"Status":"Approved"})
print(f"  Updated SharePoint item: {'success' if updated else 'failed'}")

# ── Engine stats ──────────────────────────────────────────────
print(f"\n📊 ENGINE STATS\n")
stats = engine.get_stats()
for k, v in stats.items():
    print(f"  {k:<22} {v}")

print(f"\n✅ Phase 1 complete — Graph adapter + delta sync + webhooks ready.")
print(f"   Next: Phase 2 — ETL engine (transformer, validator, error recovery)\n")
