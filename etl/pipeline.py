"""
ETL Pipeline
Orchestrates the full extract → transform → validate → load cycle.
Connects the Graph adapter output to Oracle-ready row batches.
"""

import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional

from graph.models import SPListItem, SPDocument, SyncResult
from etl.transformer import FieldTransformer, TransformRule
from etl.validator import ETLValidator, ValidationRule
from etl.error_recovery import ErrorRecoveryEngine, FailureReason


@dataclass
class PipelineResult:
    """Aggregate result of running the ETL pipeline over a batch of items."""
    resource_id:     str
    resource_type:   str
    total_input:     int = 0
    transformed_ok:  int = 0
    validated_ok:    int = 0
    loaded_ok:       int = 0
    failed:          int = 0
    dlq_count:       int = 0
    avg_quality:     float = 0.0
    warnings:        list[str] = field(default_factory=list)
    duration_ms:     float = 0.0
    started_at:      datetime = field(default_factory=datetime.utcnow)

    @property
    def success_rate(self) -> float:
        return round(self.loaded_ok / max(self.total_input, 1) * 100, 1)


class ETLPipeline:
    """
    Full ETL pipeline: SharePoint items → Oracle rows.
    Transform → Validate → Load (with error recovery at each stage).
    """

    def __init__(self, load_fn=None):
        self._transformer = FieldTransformer()
        self._validator   = ETLValidator()
        self._recovery    = ErrorRecoveryEngine(max_retries=3)
        self._load_fn     = load_fn or self._mock_load
        self._all_results: list[PipelineResult] = []

    def process_items(
        self,
        items:       list[SPListItem],
        resource_id: str,
        rules:       list[TransformRule] = None,
        val_rules:   list[ValidationRule] = None,
        strict:      bool = False,
    ) -> PipelineResult:
        """Process a batch of SharePoint list items through the full pipeline."""
        t0     = time.time()
        result = PipelineResult(resource_id=resource_id, resource_type="list_item")
        result.total_input = len(items)
        quality_scores = []

        for item in items:
            # Stage 1 — Transform
            t_result = self._transformer.transform(
                item_id=item.item_id,
                fields=item.fields,
                rules=rules,
                metadata={
                    "id":             item.item_id,
                    "etag":           item.etag,
                    "created":        item.created_at.isoformat() if item.created_at else None,
                    "modified":       item.modified_at.isoformat() if item.modified_at else None,
                    "createdBy":      item.created_by,
                    "lastModifiedBy": item.modified_by,
                },
            )
            if not t_result.success:
                result.failed += 1
                result.warnings.extend(t_result.errors)
                self._recovery._send_to_dlq(item.item_id, resource_id, "list_item",
                                             item.fields, t_result.errors[0] if t_result.errors else "transform error")
                result.dlq_count += 1
                continue
            result.transformed_ok += 1

            # Stage 2 — Validate
            v_result = self._validator.validate(
                item_id=item.item_id,
                oracle_row=t_result.oracle_row,
                rules=val_rules,
                strict=strict,
            )
            quality_scores.append(v_result.quality_score)
            if result.warnings:
                result.warnings.extend(v_result.warnings)

            if not v_result.valid:
                result.failed += 1
                self._recovery._send_to_dlq(item.item_id, resource_id, "list_item",
                                             t_result.oracle_row, v_result.errors[0])
                result.dlq_count += 1
                continue
            result.validated_ok += 1

            # Stage 3 — Load
            oracle_row = {**t_result.oracle_row, "LIST_ID": resource_id, "SITE_ID": item.site_id}
            success, _ = self._recovery.execute_with_retry(
                fn=self._load_fn,
                item_id=item.item_id,
                resource_id=resource_id,
                resource_type="list_item",
                payload=oracle_row,
                oracle_row=oracle_row,
            )
            if success:
                result.loaded_ok += 1
            else:
                result.failed += 1
                result.dlq_count += 1

        result.avg_quality = round(sum(quality_scores) / max(len(quality_scores), 1), 1)
        result.duration_ms = round((time.time() - t0) * 1000, 2)
        self._all_results.append(result)
        return result

    def process_documents(
        self,
        docs:        list[SPDocument],
        resource_id: str,
    ) -> PipelineResult:
        """Process document library items — preserves extracted text."""
        t0     = time.time()
        result = PipelineResult(resource_id=resource_id, resource_type="document")
        result.total_input = len(docs)

        for doc in docs:
            oracle_row = {
                "DOC_ID":         doc.doc_id,
                "LIST_ID":        doc.list_id,
                "SITE_ID":        doc.site_id,
                "FILE_NAME":      doc.file_name[:500],
                "FILE_TYPE":      doc.file_type,
                "FILE_SIZE":      doc.file_size,
                "WEB_URL":        doc.web_url[:2000],
                "SP_ETAG":        doc.etag,
                "EXTRACTED_TEXT": doc.extracted_text,
                "PAGE_COUNT":     doc.page_count,
                "SP_CREATED_BY":  doc.created_by,
                "SP_CREATED_AT":  doc.created_at.isoformat() if doc.created_at else None,
                "SP_MODIFIED_AT": doc.modified_at.isoformat() if doc.modified_at else None,
            }
            v_result = self._validator.validate(doc.doc_id, oracle_row)
            if not v_result.valid:
                result.failed += 1
                continue
            result.validated_ok += 1

            success, _ = self._recovery.execute_with_retry(
                fn=self._load_fn,
                item_id=doc.doc_id,
                resource_id=resource_id,
                resource_type="document",
                payload=oracle_row,
                oracle_row=oracle_row,
            )
            if success:
                result.loaded_ok += 1
                result.transformed_ok += 1
            else:
                result.failed += 1
                result.dlq_count += 1

        result.duration_ms = round((time.time() - t0) * 1000, 2)
        self._all_results.append(result)
        return result

    def get_pipeline_stats(self) -> dict:
        recovery_stats = self._recovery.get_stats()
        total_in  = sum(r.total_input  for r in self._all_results)
        total_out = sum(r.loaded_ok    for r in self._all_results)
        return {
            "pipeline_runs":   len(self._all_results),
            "total_input":     total_in,
            "total_loaded":    total_out,
            "overall_success": round(total_out / max(total_in, 1) * 100, 1),
            "recovery":        recovery_stats,
        }

    def get_dlq_summary(self) -> list[dict]:
        return [
            {
                "dlq_id":    d.dlq_id,
                "item_id":   d.item_id,
                "reason":    d.failure_reason.value,
                "error":     d.error_message[:80],
                "retries":   d.retry_count,
                "resolved":  d.resolved,
            }
            for d in self._recovery.get_dlq(resolved=False)
        ]

    def _mock_load(self, oracle_row: dict) -> bool:
        """Mock Oracle load — replace with real oracledb INSERT in Phase 3."""
        return True
