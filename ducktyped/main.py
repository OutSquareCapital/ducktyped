from dataclasses import dataclass
from typing import Self
from pathlib import Path
from ducktyped.types import DuckType
from ducktyped.enums import KeyWord
from ducktyped.expressions import Expr, Col, AllExpr


def col(name: str) -> Col:
    return Col(name=name)


def all() -> AllExpr:
    return AllExpr()


@dataclass(slots=True)
class Table:
    filepath: str | Path
    schema: dict[str, DuckType]

    def select(self, *cols: Col | Expr) -> "Query":
        return Query(table=self).select(*cols)


class Query:
    def __init__(self, table: Table) -> None:
        self._table: Table = table
        self._selected: list[Expr] = []
        self._where_clause: list[Expr] = []

    def __repr__(self) -> str:
        return f"{self.to_sql().splitlines()}"

    def select(self, *cols: Expr) -> Self:
        self._selected.extend(cols)
        return self

    def where(self, condition: Expr) -> Self:
        self._where_clause.append(condition)
        return self

    def to_sql(self) -> str:
        select_sql: str = ", ".join(col.to_sql() for col in self._selected)
        where_sql: str = ""
        if self._where_clause:
            where_sql: str = f" {KeyWord.WHERE} " + f" {KeyWord.AND} ".join(
                cond.to_sql() for cond in self._where_clause
            )
        return f"{KeyWord.SELECT} {select_sql} {KeyWord.FROM} '{self._table.filepath}'{where_sql}"
