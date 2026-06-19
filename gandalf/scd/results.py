"""Structured merge results for optional observability."""

from dataclasses import dataclass, field


@dataclass(frozen=True)
class MergeResult:
    path: str
    merge_type: str
    inserted_rows: int | None = None
    updated_rows: int | None = None
    deleted_rows: int | None = None
    source_rows: int | None = None
    target_current_rows: int | None = None
    warnings: list[str] = field(default_factory=list)
