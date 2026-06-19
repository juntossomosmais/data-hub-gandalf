"""Safe SQL predicate builders for Delta merge conditions."""

import re

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def validate_identifier(name: str) -> str:
    """Validate a SQL identifier fragment used for generated Delta merge predicates."""
    if not isinstance(name, str) or not _IDENTIFIER_RE.fullmatch(name):
        raise ValueError(
            f"Unsafe SQL identifier '{name}'. Only simple identifiers matching "
            r"^[A-Za-z_][A-Za-z0-9_]*$ are allowed."
        )
    return name


def quote_identifier(name: str) -> str:
    """Backtick-quote a validated identifier."""
    return f"`{validate_identifier(name)}`"


def _validate_alias(alias: str) -> str:
    return validate_identifier(alias)


def build_key_condition(target_alias: str, source_alias: str, ids: list[str]) -> str:
    """Build a composite equality predicate between target and source aliases."""
    if not ids:
        raise ValueError("ids must be a non-empty list")
    target = _validate_alias(target_alias)
    source = _validate_alias(source_alias)
    return " AND ".join(f"{target}.{quote_identifier(id_)} = {source}.{quote_identifier(id_)}" for id_ in ids)


def build_current_key_condition(
    target_alias: str,
    source_alias_or_merge_alias: str,
    ids: list[str],
    current_flag_col: str,
) -> str:
    """Build key equality plus target current-record predicate."""
    base = build_key_condition(target_alias, source_alias_or_merge_alias, ids)
    target = _validate_alias(target_alias)
    return f"{base} AND {target}.{quote_identifier(current_flag_col)} = 1"


def append_extra_condition(base: str, extra: str | None) -> str:
    """Append a legacy raw SQL extra condition to a generated base predicate."""
    if not extra:
        return base
    return f"{base} AND ({extra})"
