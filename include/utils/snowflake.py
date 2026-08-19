"""Shared Snowflake helpers used by operators and triggers.

Kept in a standalone module to avoid a circular import between
``include.operators.snowflake_operators`` (which imports
``SnowflakeTaskTrigger``) and ``include.triggers.snowflake_task_trigger``
(which needs the same fully-qualified-name validator).
"""

from __future__ import annotations

import re

_IDENT = r'(?:[A-Za-z_][A-Za-z0-9_$]*|"[^"]+")'
_FQ_TASK_RE = re.compile(rf"^{_IDENT}\.{_IDENT}\.{_IDENT}$")


def validate_fq_task_name(name: str) -> str:
    """Validate a ``DATABASE.SCHEMA.TASK`` Snowflake identifier.

    Raises ``ValueError`` on anything that does not match three
    ``IDENT``-shaped parts separated by dots. Accepts both unquoted
    identifiers (case-folded by Snowflake) and quoted identifiers.
    """
    if not isinstance(name, str) or not _FQ_TASK_RE.match(name):
        raise ValueError(
            f"Invalid fully qualified Snowflake task name: {name!r}. "
            "Expected DATABASE.SCHEMA.TASK."
        )
    return name


def ident_forms(part: str) -> tuple[str, str]:
    """Return ``(sql_form, python_value_form)`` for one identifier part.

    - Quoted identifiers keep their quotes for SQL and drop them for value
      comparisons against ``INFORMATION_SCHEMA``.
    - Unquoted identifiers are upper-cased in both forms to match how
      Snowflake stores them in ``INFORMATION_SCHEMA``.
    """
    if part.startswith('"') and part.endswith('"'):
        return part, part[1:-1]

    upper = part.upper()
    return upper, upper


__all__ = [
    "validate_fq_task_name",
    "ident_forms",
]
