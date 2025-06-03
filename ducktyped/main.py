from pathlib import Path
from ducktyped.expressions import Expr
from ducktyped.cols import Col
from ducktyped.query import Query
from dataclasses import dataclass


def _selection(*cols: Col | Expr) -> list[Expr]:
    _selected: list[Expr] = []
    for c in cols:
        _selected.append(c)
    return _selected


class SELECT:
    __slots__ = ("_selected",)
    def __init__(self, *cols: Col | Expr) -> None:
        self._selected: list[Expr] = _selection(*cols)

    def FROM(self, table: Path) -> Query:
        return Query(table=table, selected=self._selected)


@dataclass(slots=True)
class TABLE:
    name: str
    path: Path

    def SELECT(self, *cols: Col | Expr) -> Query:
        return Query(table=self.path, selected=_selection(*cols))
