"""
Field Transformer
Maps SharePoint field types and values to Oracle-compatible formats.
Handles type coercion, date normalisation, choice fields, person fields, and lookups.
"""

import re
import json
from datetime import datetime
from typing import Any, Optional
from dataclasses import dataclass, field


@dataclass
class TransformRule:
    """A single field transformation rule."""
    sp_field:     str              # SharePoint field name
    oracle_col:   str              # Target Oracle column name
    sp_type:      str              # text | number | dateTime | boolean | choice | person | lookup | url
    oracle_type:  str              # VARCHAR2 | NUMBER | DATE | CLOB
    max_length:   Optional[int] = None
    required:     bool = False
    default:      Any  = None
    transform_fn: Optional[str] = None   # Name of custom transform function


@dataclass
class TransformResult:
    """Result of transforming one SharePoint item."""
    item_id:       str
    success:       bool
    oracle_row:    dict[str, Any] = field(default_factory=dict)
    warnings:      list[str]      = field(default_factory=list)
    errors:        list[str]      = field(default_factory=list)
    fields_mapped: int = 0
    fields_skipped:int = 0


class FieldTransformer:
    """
    Transforms SharePoint field values into Oracle-ready row dictionaries.
    Handles all SharePoint field types cleanly.
    """

    # Default mapping: SharePoint type → Oracle type
    TYPE_MAP = {
        "text":     ("VARCHAR2", 4000),
        "note":     ("CLOB",     None),
        "number":   ("NUMBER",   None),
        "currency": ("NUMBER",   None),
        "dateTime": ("DATE",     None),
        "boolean":  ("NUMBER",   1),     # Oracle has no BOOLEAN — use 0/1
        "choice":   ("VARCHAR2", 255),
        "person":   ("VARCHAR2", 255),
        "lookup":   ("VARCHAR2", 255),
        "url":      ("VARCHAR2", 2000),
        "computed": ("VARCHAR2", 1000),
        "integer":  ("NUMBER",   None),
    }

    # SharePoint reserved fields we always carry through
    SYSTEM_FIELDS = {
        "id":           ("SP_ITEM_ID",    "VARCHAR2", 100),
        "etag":         ("SP_ETAG",       "VARCHAR2", 50),
        "created":      ("SP_CREATED_AT", "DATE",     None),
        "modified":     ("SP_MODIFIED_AT","DATE",     None),
        "createdBy":    ("SP_CREATED_BY", "VARCHAR2", 255),
        "lastModifiedBy":("SP_MODIFIED_BY","VARCHAR2",255),
    }

    def transform(
        self,
        item_id:    str,
        fields:     dict[str, Any],
        rules:      list[TransformRule] = None,
        metadata:   dict = None,
    ) -> TransformResult:
        """
        Transform a SharePoint item's fields into an Oracle row.
        Uses explicit rules if provided, otherwise auto-maps.
        """
        result = TransformResult(item_id=item_id, success=True)
        metadata = metadata or {}

        # System fields from item metadata
        for sp_key, (oracle_col, oracle_type, _) in self.SYSTEM_FIELDS.items():
            val = metadata.get(sp_key)
            if val is not None:
                result.oracle_row[oracle_col] = self._coerce(val, oracle_type)

        if rules:
            self._apply_rules(fields, rules, result)
        else:
            self._auto_map(fields, result)

        return result

    def _apply_rules(self, fields: dict, rules: list[TransformRule], result: TransformResult):
        """Apply explicit field mapping rules."""
        for rule in rules:
            raw_val = fields.get(rule.sp_field)

            if raw_val is None:
                if rule.required:
                    result.errors.append(f"Required field missing: {rule.sp_field}")
                    result.success = False
                elif rule.default is not None:
                    result.oracle_row[rule.oracle_col] = rule.default
                    result.fields_mapped += 1
                else:
                    result.fields_skipped += 1
                continue

            try:
                coerced = self._coerce(raw_val, rule.oracle_type, rule.max_length)
                result.oracle_row[rule.oracle_col] = coerced
                result.fields_mapped += 1
            except Exception as e:
                result.warnings.append(f"Field {rule.sp_field} coercion failed: {e} — using NULL")
                result.oracle_row[rule.oracle_col] = None
                result.fields_skipped += 1

    def _auto_map(self, fields: dict, result: TransformResult):
        """Auto-map all fields — detect types and coerce."""
        for sp_field, raw_val in fields.items():
            if raw_val is None:
                result.fields_skipped += 1
                continue

            oracle_col = self._sp_to_oracle_name(sp_field)
            detected   = self._detect_type(raw_val)
            oracle_type, _ = self.TYPE_MAP.get(detected, ("VARCHAR2", 4000))

            try:
                coerced = self._coerce(raw_val, oracle_type)
                result.oracle_row[oracle_col] = coerced
                result.fields_mapped += 1
            except Exception:
                result.oracle_row[oracle_col] = str(raw_val)[:4000]
                result.fields_mapped += 1

    def _coerce(self, val: Any, oracle_type: str, max_length: int = None) -> Any:
        if val is None:
            return None

        if oracle_type == "DATE":
            return self._parse_date(val)
        if oracle_type == "NUMBER":
            return self._parse_number(val)
        if oracle_type in ("VARCHAR2", "CLOB"):
            s = self._parse_string(val)
            if max_length and len(s) > max_length:
                return s[:max_length]
            return s
        return val

    def _parse_date(self, val: Any) -> Optional[str]:
        if isinstance(val, datetime):
            return val.strftime("%Y-%m-%d %H:%M:%S")
        if isinstance(val, str):
            # Try common formats
            for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%d/%m/%Y"):
                try:
                    return datetime.strptime(val, fmt).strftime("%Y-%m-%d %H:%M:%S")
                except ValueError:
                    continue
        return None

    def _parse_number(self, val: Any) -> Optional[float]:
        try:
            if isinstance(val, bool):
                return 1 if val else 0
            return float(str(val).replace(",", ""))
        except (ValueError, TypeError):
            return None

    def _parse_string(self, val: Any) -> str:
        if isinstance(val, dict):
            # Person field: {"displayName": "Alice", "email": "alice@..."}
            return val.get("email") or val.get("displayName") or json.dumps(val)
        if isinstance(val, list):
            # Multi-select choice or lookup
            return "; ".join(str(v) for v in val)
        return str(val)

    def _detect_type(self, val: Any) -> str:
        if isinstance(val, bool):        return "boolean"
        if isinstance(val, int):         return "integer"
        if isinstance(val, float):       return "number"
        if isinstance(val, dict):        return "person"
        if isinstance(val, list):        return "choice"
        if isinstance(val, str):
            if re.match(r"\d{4}-\d{2}-\d{2}", val): return "dateTime"
            if val.startswith("http"):               return "url"
            if len(val) > 512:                       return "note"
        return "text"

    def _sp_to_oracle_name(self, sp_name: str) -> str:
        """Convert SharePoint camelCase/PascalCase to ORACLE_SNAKE_CASE."""
        name = re.sub(r"([A-Z])", r"_\1", sp_name).upper().lstrip("_")
        name = re.sub(r"[^A-Z0-9_]", "_", name)
        return name[:30]  # Oracle identifier max = 30 chars
