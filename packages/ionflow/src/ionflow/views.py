"""Presentation hints over declared schemas; execution dependencies remain explicit."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .artifact import Artifact


@dataclass(frozen=True)
class View:
    """Optional schema hints for a generated browser.

    >>> View(label="RawDB", plural="RawDBs", columns=("name", "local")).plural
    'RawDBs'
    """

    label: str | None = None
    plural: str | None = None
    columns: tuple[str, ...] = ()
    labels: dict[str, str] = field(default_factory=dict)
    formats: dict[str, str] = field(default_factory=dict)
    references: dict[str, type[Artifact]] = field(default_factory=dict)

    def describe(self, fields: dict[str, Any]) -> dict:
        """Validate hints against a schema and return JSON-compatible view metadata."""
        from .artifact import Artifact

        selected = set(self.columns) | self.labels.keys() | self.formats.keys() | self.references.keys()
        if unknown := selected - fields.keys():
            raise ValueError(f"view names unknown fields: {sorted(unknown)}")
        allowed = {"integer", "number", "bytes", "duration", "datetime", "text"}
        if unknown_formats := set(self.formats.values()) - allowed:
            raise ValueError(f"unknown view formats: {sorted(unknown_formats)}")
        for name, target in self.references.items():
            if not isinstance(target, type) or not issubclass(target, Artifact):
                raise TypeError(f"reference {name!r} must target an Artifact type")
            if fields[name] not in (str, list[str]):
                raise TypeError(f"reference {name!r} requires a string or string-list field")
        return {
            "label": self.label,
            "plural": self.plural,
            "columns": list(self.columns or fields),
            "labels": dict(self.labels),
            "formats": dict(self.formats),
            "references": {name: target._kind for name, target in self.references.items()},
        }
