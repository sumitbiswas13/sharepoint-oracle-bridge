"""
SharePoint Oracle Bridge — Phase 2 Demo
ETL engine: transformer + validator + error recovery.
Run with: python demo/demo_phase2.py
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from graph.adapter import MockGraphAdapter
from graph.delta_sync import DeltaSyncEngine
from etl.transformer import FieldTransformer, TransformRule
from etl.validator import ETLValidator, ValidationRule
from etl.error_recovery import ErrorRecoveryEngine, FailureReason
from etl.pipeline import ETLPipeline

adapter = MockGraphAdapter()
engine  = DeltaSyncEngine(adapter)
pipe    = ETLPipeline()

print("=" * 65)
print("  SharePoint Oracle Bridge — Phase 2")
print("  ETL Engine: Transform → Validate → Load with Error Recovery")
print("=" * 65)

# ── Fetch items ───────────────────────────────────────────────
print("\n📥 FETCHING FROM SHAREPOINT\n")
items, token = adapter.get_list_items("site-001", "list-001")
docs         = adapter.get_documents("site-002", "list-006")
print(f"  List items fetched : {len(items)}")
print(f"  Documents fetched  : {len(docs)}")
print(f"  Delta token        : {token[:16]}...")

# ── Transform demo ────────────────────────────────────────────
print("\n🔄 FIELD TRANSFORMER\n")
transformer = FieldTransformer()

for item in items[:3]:
    t_result = transformer.transform(
        item_id=item.item_id,
        fields=item.fields,
        metadata={
            "id": item.item_id, "etag": item.etag,
            "created": item.created_at.isoformat() if item.created_at else None,
            "modified": item.modified_at.isoformat() if item.modified_at else None,
            "createdBy": item.created_by, "lastModifiedBy": item.modified_by,
        },
    )
    print(f"  {item.item_id}  mapped={t_result.fields_mapped}  skipped={t_result.fields_skipped}")
    for k, v in list(t_result.oracle_row.items())[:5]:
        print(f"    {k:<25} = {str(v)[:40]}")

# ── Validation demo ───────────────────────────────────────────
print(f"\n✅ VALIDATOR\n")
validator = ETLValidator()
test_rows = [
    {"SP_ITEM_ID": "item-001", "TITLE": "Q4 Planning",   "STATUS": "Approved",  "PRIORITY": "High"},
    {"SP_ITEM_ID": "item-002", "TITLE": "X" * 5000,      "STATUS": "Unknown",   "PRIORITY": "Critical"},
    {"SP_ITEM_ID": "item-003", "TITLE": "",               "STATUS": "Pending",   "PRIORITY": "Low"},
]
extra_rules = [ValidationRule(field="TITLE", rule_type="required")]
for row in test_rows:
    v = validator.validate(row["SP_ITEM_ID"], row, rules=extra_rules)
    icon = "✅" if v.valid else "❌"
    print(f"  {icon} {row['SP_ITEM_ID']}  quality={v.quality_score:.0f}%  "
          f"errors={len(v.errors)}  warnings={len(v.warnings)}")
    for e in v.errors:   print(f"     ERROR: {e}")
    for w in v.warnings: print(f"     WARN : {w}")

# ── Error recovery + circuit breaker ─────────────────────────
print(f"\n🔁 ERROR RECOVERY + CIRCUIT BREAKER\n")
recovery  = ErrorRecoveryEngine(max_retries=2)
call_count = [0]

def flaky_load(oracle_row):
    call_count[0] += 1
    if call_count[0] <= 3:
        raise ConnectionError("DB connection refused")
    return True

# Items that will fail and hit DLQ
for i in range(4):
    success, result = recovery.execute_with_retry(
        fn=flaky_load,
        item_id=f"item-{i+1:03d}",
        resource_id="list-001",
        resource_type="list_item",
        payload={"TITLE": f"Item {i+1}"},
        oracle_row={"TITLE": f"Item {i+1}"},
    )
    icon = "✅" if success else "❌"
    print(f"  {icon} item-{i+1:03d}  success={success}")

print(f"\n  Circuit breaker: {recovery.get_circuit_status()}")
dlq = recovery.get_dlq(resolved=False)
print(f"  DLQ items: {len(dlq)}")
for d in dlq:
    print(f"    {d.dlq_id}  {d.item_id}  reason={d.failure_reason.value}  retries={d.retry_count}")

# ── Full pipeline ─────────────────────────────────────────────
print(f"\n🚀 FULL PIPELINE — list items\n")
result = pipe.process_items(items, resource_id="list-001")
print(f"  Input       : {result.total_input}")
print(f"  Transformed : {result.transformed_ok}")
print(f"  Validated   : {result.validated_ok}")
print(f"  Loaded      : {result.loaded_ok}")
print(f"  Failed      : {result.failed}")
print(f"  DLQ         : {result.dlq_count}")
print(f"  Quality     : {result.avg_quality}%")
print(f"  Success rate: {result.success_rate}%")
print(f"  Duration    : {result.duration_ms:.0f}ms")

print(f"\n🚀 FULL PIPELINE — documents\n")
doc_result = pipe.process_documents(docs, resource_id="list-006")
print(f"  Input       : {doc_result.total_input}")
print(f"  Loaded      : {doc_result.loaded_ok}")
print(f"  Duration    : {doc_result.duration_ms:.0f}ms")

print(f"\n📊 OVERALL PIPELINE STATS\n")
stats = pipe.get_pipeline_stats()
for k, v in stats.items():
    if k != "recovery":
        print(f"  {k:<22} {v}")
rec = stats["recovery"]
print(f"  {'dlq_pending':<22} {rec['dlq_pending']}")
print(f"  {'retry_attempts':<22} {rec['retry_attempts']}")

print(f"\n✅ Phase 2 complete — ETL engine ready.")
print(f"   Next: Phase 3 — Oracle schema + bidirectional sync\n")
