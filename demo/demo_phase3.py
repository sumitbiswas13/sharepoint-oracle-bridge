"""
SharePoint Oracle Bridge — Phase 3 Demo
Oracle schema + bidirectional sync.
Run with: python demo/demo_phase3.py
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from graph.adapter import MockGraphAdapter
from oracle.executor import MockOracleExecutor
from oracle.bidirectional import BidirectionalSyncEngine
from oracle.schema import SCHEMA_DDL

adapter = MockGraphAdapter()
oracle  = MockOracleExecutor()
bidir   = BidirectionalSyncEngine(adapter, oracle)

print("=" * 65)
print("  SharePoint Oracle Bridge — Phase 3")
print("  Oracle Schema + Bidirectional Sync")
print("=" * 65)

# ── Schema preview ────────────────────────────────────────────
print("\n📋 ORACLE SCHEMA — SPX_* TABLES\n")
tables = ["SPX_SITES","SPX_LISTS","SPX_ITEMS","SPX_DOCUMENTS","SPX_SYNC_LOG"]
descs  = [
    "One row per SharePoint site",
    "One row per SharePoint list or document library",
    "One row per list item — FIELDS_JSON + PUSH_PENDING flag",
    "Document library files with extracted text in CLOB",
    "Audit trail for every sync operation",
]
for t, d in zip(tables, descs):
    print(f"  {t:<22} — {d}")

# ── Full SP → Oracle sync ─────────────────────────────────────
print(f"\n{'─'*65}")
print("  DIRECTION 1: SharePoint → Oracle (full sync)")
print(f"{'─'*65}\n")
results = bidir.sync_sp_to_oracle("site-001", full=True)
stats   = oracle.get_stats()
print(f"\n  Oracle table row counts after full sync:")
for k, v in stats.items():
    if k != "push_pending":
        print(f"    {k:<22} {v} rows")

# ── Delta SP → Oracle ─────────────────────────────────────────
print(f"\n  Running delta sync (same site — should be much smaller)...")
bidir.sync_sp_to_oracle("site-001", full=False)
stats2 = oracle.get_stats()
print(f"  SPX_ITEMS after delta : {stats2['SPX_ITEMS']} rows (only changed items updated)")

# ── Oracle → SP push ──────────────────────────────────────────
print(f"\n{'─'*65}")
print("  DIRECTION 2: Oracle → SharePoint (bidirectional push)")
print(f"{'─'*65}\n")

# Simulate Oracle-originated items that need pushing to SharePoint
oracle_items = [
    ("oracle-payroll-001", "list-001", "site-001", {
        "TITLE": "November Payroll Completed", "STATUS": "Complete", "PRIORITY": "High",
    }),
    ("oracle-payroll-002", "list-001", "site-001", {
        "TITLE": "Australia Payroll Run", "STATUS": "InProgress", "PRIORITY": "Medium",
    }),
    ("oracle-audit-001", "list-003", "site-001", {
        "TITLE": "Q4 Compliance Report Ready", "STATUS": "Approved", "PRIORITY": "High",
    }),
]

for item_id, list_id, site_id, fields in oracle_items:
    bidir.queue_for_sharepoint(item_id, list_id, site_id, fields)
    print(f"  Queued: {item_id} → {list_id}  [{fields['STATUS']}]")

pending = oracle.get_items(push_pending=True)
print(f"\n  PUSH_PENDING items in Oracle: {len(pending)}")

push_result = bidir.sync_oracle_to_sp()
print(f"\n  Push result:")
print(f"    Items found   : {push_result.items_found}")
print(f"    Created in SP : {push_result.items_created}")
print(f"    Updated in SP : {push_result.items_updated}")
print(f"    Failed        : {push_result.items_failed}")
print(f"    Duration      : {push_result.duration_ms:.0f}ms")

remaining = oracle.get_items(push_pending=True)
print(f"\n  PUSH_PENDING after sync: {len(remaining)} (cleared on success)")

# ── Sync log ──────────────────────────────────────────────────
print(f"\n{'─'*65}")
print("  ORACLE SYNC AUDIT LOG (SPX_SYNC_LOG)")
print(f"{'─'*65}\n")
log = oracle.get_sync_log(limit=8)
print(f"  {'LOG_ID':>6} {'OPERATION':<22} {'RESOURCE':<16} {'CREATED':>8} {'UPDATED':>8} {'DIR':<14} {'MS':>6}")
print(f"  {'─'*6} {'─'*22} {'─'*16} {'─'*8} {'─'*8} {'─'*14} {'─'*6}")
for row in log:
    print(f"  {row['LOG_ID']:>6} {row['OPERATION']:<22} {row['RESOURCE_ID'][:15]:<16} "
          f"{row['ITEMS_CREATED']:>8} {row['ITEMS_UPDATED']:>8} "
          f"{row['DIRECTION']:<14} {int(row['DURATION_MS'] or 0):>6}")

# ── Final stats ───────────────────────────────────────────────
print(f"\n📊 BIDIRECTIONAL SYNC STATS\n")
final = bidir.get_stats()
print(f"  Oracle table rows : {final['oracle_stats']}")
print(f"  Total sync runs   : {final['sync_runs']}")
print(f"  SP → Oracle runs  : {final['sp_to_oracle']}")
print(f"  Oracle → SP runs  : {final['oracle_to_sp']}")
print(f"  Push pending      : {final['oracle_stats']['push_pending']}")

print(f"\n✅ Phase 3 complete — Oracle schema + bidirectional sync ready.")
print(f"   Next: Phase 4 — FastAPI layer\n")
