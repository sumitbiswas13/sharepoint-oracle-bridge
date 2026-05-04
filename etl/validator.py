"""
ETL Validator
Validates transformed rows against Oracle schema constraints and business rules.
Catches data quality issues before they reach the database.
"""

from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class ValidationRule:
    field:      str
    rule_type:  str     # required | max_length | regex | allowed_values | min | max | not_null
    value:      Any = None
    message:    str = ""


@dataclass
class ValidationResult:
    item_id:   str
    valid:     bool
    errors:    list[str] = field(default_factory=list)
    warnings:  list[str] = field(default_factory=list)
    quality_score: float = 100.0   # 0–100


class ETLValidator:
    """
    Validates transformed Oracle rows before load.
    Two levels: hard errors (block the row) and warnings (pass with flag).
    """

    # Built-in Oracle type constraints
    TYPE_CONSTRAINTS = {
        "VARCHAR2": {"max_len": 4000},
        "NUMBER":   {"min": -1e38, "max": 1e38},
        "DATE":     {"pattern": r"\d{4}-\d{2}-\d{2}"},
        "CLOB":     {},
    }

    ALLOWED_STATUSES = {"Approved","InProgress","Pending","Complete","Draft","Cancelled","On Hold"}
    ALLOWED_PRIORITIES = {"Critical","High","Medium","Low","None"}

    def validate(
        self,
        item_id:    str,
        oracle_row: dict[str, Any],
        rules:      list[ValidationRule] = None,
        strict:     bool = False,
    ) -> ValidationResult:
        result = ValidationResult(item_id=item_id, valid=True)
        checks_passed = 0
        checks_total  = 0

        # Built-in type validation
        for col, val in oracle_row.items():
            checks_total += 1
            if val is None:
                checks_passed += 1
                continue

            if isinstance(val, str) and len(val) > 4000:
                result.errors.append(f"{col}: VARCHAR2 value exceeds 4000 chars ({len(val)})")
                result.valid = False
            elif isinstance(val, str) and col.endswith("_AT"):
                import re
                if not re.match(r"\d{4}-\d{2}-\d{2}", val):
                    result.warnings.append(f"{col}: date format unexpected '{val[:20]}'")
                else:
                    checks_passed += 1
            else:
                checks_passed += 1

        # Business rule validation on common fields
        status_val = oracle_row.get("STATUS")
        if status_val and isinstance(status_val, str):
            checks_total += 1
            if status_val not in self.ALLOWED_STATUSES:
                result.warnings.append(f"STATUS '{status_val}' not in standard values — will load as-is")
            else:
                checks_passed += 1

        priority_val = oracle_row.get("PRIORITY")
        if priority_val and isinstance(priority_val, str):
            checks_total += 1
            if priority_val not in self.ALLOWED_PRIORITIES:
                result.warnings.append(f"PRIORITY '{priority_val}' not standard — will load as-is")
            else:
                checks_passed += 1

        # Custom rules
        for rule in (rules or []):
            checks_total += 1
            ok, msg = self._check_rule(rule, oracle_row)
            if ok:
                checks_passed += 1
            else:
                full_msg = msg or rule.message or f"{rule.field}: {rule.rule_type} check failed"
                if rule.rule_type == "required" or strict:
                    result.errors.append(full_msg)
                    result.valid = False
                else:
                    result.warnings.append(full_msg)

        # Quality score
        result.quality_score = round(checks_passed / max(checks_total, 1) * 100, 1)
        if result.errors:
            result.valid = False

        return result

    def _check_rule(self, rule: ValidationRule, row: dict) -> tuple[bool, str]:
        val = row.get(rule.field)

        if rule.rule_type == "required":
            if val is None or val == "":
                return False, f"{rule.field} is required but missing"
            return True, ""

        if rule.rule_type == "max_length":
            if val and isinstance(val, str) and len(val) > rule.value:
                return False, f"{rule.field} too long ({len(val)} > {rule.value})"
            return True, ""

        if rule.rule_type == "regex":
            import re
            if val and not re.match(rule.value, str(val)):
                return False, f"{rule.field} failed pattern check"
            return True, ""

        if rule.rule_type == "allowed_values":
            if val and val not in rule.value:
                return False, f"{rule.field} value '{val}' not in allowed set"
            return True, ""

        if rule.rule_type == "min":
            try:
                if val is not None and float(val) < rule.value:
                    return False, f"{rule.field} {val} below minimum {rule.value}"
            except (TypeError, ValueError):
                pass
            return True, ""

        if rule.rule_type == "max":
            try:
                if val is not None and float(val) > rule.value:
                    return False, f"{rule.field} {val} above maximum {rule.value}"
            except (TypeError, ValueError):
                pass
            return True, ""

        if rule.rule_type == "not_null":
            if val is None:
                return False, f"{rule.field} must not be null"
            return True, ""

        return True, ""
